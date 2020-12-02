package cbidxr_test

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/dcarbone/go-cbidxr"
)

const (
	envCouchbaseAddress  = "COUCHBASE_ADDRESS"
	envCouchbaseBucket   = "COUCHBASE_BUCKET"
	envCouchbaseUsername = "COUCHBASE_USERNAME"
	envCouchbasePassword = "COUCHBASE_PASSWORD"

	defaultIDXPrefix  = "cbidx-test-"
	defaultBucketName = "default"
)

type testCouchbaseConfig struct {
	Address  string `json:"address"`
	Bucket   string `json:"bucket"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type testTestConfig struct {
	Name         string                    `json:"name"`
	IndexPrefix  string                    `json:"index_prefix"`
	InitialState []*cbidxr.IndexDefinition `json:"initial_state"`
	FinalState   []*cbidxr.IndexDefinition `json:"final_state"`
}

type testConfig struct {
	Couchbase *testCouchbaseConfig `json:"couchbase"`
	Tests     []*testTestConfig    `json:"tests"`
}

func buildCouchbaseTestConfig() *testCouchbaseConfig {
	tc := testCouchbaseConfig{
		Address:  os.Getenv(envCouchbaseAddress),
		Bucket:   os.Getenv(envCouchbaseBucket),
		Username: os.Getenv(envCouchbaseUsername),
		Password: os.Getenv(envCouchbasePassword),
	}
	if tc.Address == "" {
		tc.Address = "127.0.0.1"
	}
	if tc.Bucket == "" {
		tc.Bucket = defaultBucketName
	}
	return &tc
}

var theTests *testConfig

func init() {
	cbConf := buildCouchbaseTestConfig()
	theTests = &testConfig{
		Couchbase: cbConf,
		Tests: []*testTestConfig{
			{
				Name:        "does-it-work",
				IndexPrefix: defaultIDXPrefix,
				InitialState: []*cbidxr.IndexDefinition{
					{
						Name:         defaultIDXPrefix + "idx1",
						KeyspaceID:   cbConf.Bucket,
						IndexKey:     []string{"`field1`", "`field2`"},
						Condition:    "`_type` = \"sandwiches\"",
						Using:        cbidxr.IndexDefaultUsing,
						NumReplica:   0,
						DeferBuild:   true,
						ForceRebuild: false,
					},
				},
				FinalState: []*cbidxr.IndexDefinition{
					{
						Name:         defaultIDXPrefix + "idx2",
						KeyspaceID:   cbConf.Bucket,
						IndexKey:     []string{"`field3`", "`field4`"},
						Condition:    "`_type` != \"sandwiches\"",
						Using:        cbidxr.IndexDefaultUsing,
						NumReplica:   0,
						DeferBuild:   true,
						ForceRebuild: false,
					},
				},
			},
		},
	}
}

func buildCluster(t *testing.T, cbConf *testCouchbaseConfig) *gocb.Cluster {
	opts := gocb.ClusterOptions{
		Authenticator: &gocb.PasswordAuthenticator{
			Username: cbConf.Username,
			Password: cbConf.Password,
		},
	}
	cluster, err := gocb.Connect(fmt.Sprintf("couchbase://%s", cbConf.Address), opts)
	if err != nil {
		t.Fatalf("error connecting to couchbase: %v", err)
	}
	if err := cluster.WaitUntilReady(5*time.Second, &gocb.WaitUntilReadyOptions{
		DesiredState: gocb.ClusterStateOnline,
		ServiceTypes: []gocb.ServiceType{
			gocb.ServiceTypeQuery,
		},
	}); err != nil {
		t.Fatalf("cluster was not ready in time: %v", err)
	}
	return cluster
}

func buildReconciler(t *testing.T, cluster *gocb.Cluster, atest *testTestConfig) *cbidxr.Reconciler {
	defConfig := cbidxr.DefaultReconcilerConfig(cluster, cbidxr.PrefixIndexLocatorFunc(atest.IndexPrefix, nil))
	defConfig.Logger = log.New(os.Stdout, fmt.Sprintf("-> test-%s ", atest.Name), log.LstdFlags|log.Lmsgprefix)
	defConfig.Debug = true
	rc, err := cbidxr.NewReconciler(defConfig)
	if err != nil {
		t.Fatalf("error creating reconciler for test config %q: %v", atest.Name, err)
	}
	return rc
}

func TestReconciler(t *testing.T) {
	for _, atest := range theTests.Tests {
		t.Run(atest.Name, func(t *testing.T) {
			cluster := buildCluster(t, theTests.Couchbase)
			if cluster == nil {
				return
			}
			defer cluster.Close(nil)
			rc := buildReconciler(t, cluster, atest)
			if err := rc.RegisterDefinitions(atest.InitialState...); err != nil {
				t.Logf("Error registering initial state definitions: %v", err)
				t.FailNow()
				return
			}
			res, err := rc.Execute()
			if err != nil {
				t.Logf("Error duging execution: %v", err)
				t.FailNow()
				return
			}
			b, _ := json.Marshal(res)
			t.Logf("results=%s", string(b))
			rc = buildReconciler(t, cluster, atest)
			if err := rc.RegisterDefinitions(atest.FinalState...); err != nil {
				t.Logf("error registering final state definitions; %v", err)
				t.FailNow()
				return
			}
			res, err = rc.Execute()
			if err != nil {
				t.Logf("error during execution: %v", err)
				t.FailNow()
				return
			}
			b, _ = json.Marshal(res)
			t.Logf("results=%s", string(b))
		})
	}
}
