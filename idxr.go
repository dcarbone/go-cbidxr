package cbidxr

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/couchbase/gocb/v2"
)

const (
	IndexDefaultUsing = "gsi"

	DefaultListQueryFormat   = "SELECT `indexes`.* FROM system:indexes WHERE `name` LIKE \"%s%%\""
	DefaultCreateQueryFormat = "CREATE INDEX `%s` ON `%s`(%s) WHERE (%s) USING %s WITH {\"num_replica\": %d, \"defer_build\": %t}"
)

type IndexAction uint8

const (
	IndexActionNoop IndexAction = iota
	IndexActionDelete
	IndexActionRecreate
	IndexActionCreate
)

func (a IndexAction) String() string {
	switch a {
	case IndexActionNoop:
		return "noop"
	case IndexActionDelete:
		return "delete"
	case IndexActionRecreate:
		return "recreate"
	case IndexActionCreate:
		return "create"
	default:
		return "unknown"
	}
}

// IndexActionDecision describes the outcome of the DecisionFunc
type IndexActionDecision struct {
	// Name is the name of the index being acted upon
	Name string

	// Action describes what will happen to this index
	Action IndexAction

	// CurrentDefinition will be populated if an index with this name already exists within Couchbase
	CurrentDefinition *IndexDefinition

	// NewDefinition will be populated if an index with this name is present in the new DefinitionMap provided to the
	// parent Reconciler
	NewDefinition *IndexDefinition
}

type IndexDefinition struct {
	Name         string   `json:"name" hcl:"name"`
	KeyspaceID   string   `json:"keyspace_id" hcl:"keyspace_id"`
	IndexKeys    []string `json:"index_keys" hcl:"index_keys"`
	Condition    string   `json:"condition" hcl:"condition"`
	Using        string   `json:"using" hcl:"using"`
	NumReplica   uint     `json:"num_replica" hcl:"num_replica"`
	DeferBuild   bool     `json:"defer_build" hcl:"defer_build"`
	ForceRebuild bool     `json:"force_rebuild" hcl:"force_rebuild"`
}

func (def *IndexDefinition) clone() IndexDefinition {
	if def == nil {
		return IndexDefinition{}
	}
	tmp := new(IndexDefinition)
	*tmp = *def
	tmp.IndexKeys = make([]string, len(def.IndexKeys))
	copy(tmp.IndexKeys, def.IndexKeys)
	return *tmp
}

type IndexDefinitionMap struct {
	mu          sync.RWMutex
	defs        map[string]IndexDefinition
	hasDeferred bool
}

func NewIndexDefinitionMap(defs ...IndexDefinition) (*IndexDefinitionMap, error) {
	dm := new(IndexDefinitionMap)
	for i, def := range defs {
		if err := dm.Register(def); err != nil {
			return nil, fmt.Errorf("init-time index definition %d has error: %w", i, err)
		}
	}
	return dm, nil
}

// Register validates the provided definition and then adds to the map
func (dm *IndexDefinitionMap) Register(def IndexDefinition) error {
	// build actual entry
	actual := new(IndexDefinition)
	actual.Name = strings.TrimSpace(def.Name)
	actual.KeyspaceID = strings.TrimSpace(def.KeyspaceID)
	actual.IndexKeys = make([]string, len(def.IndexKeys))
	copy(actual.IndexKeys, def.IndexKeys)
	actual.Condition = strings.TrimSpace(def.Condition)
	actual.Using = strings.TrimSpace(def.Using)
	actual.NumReplica = def.NumReplica
	actual.DeferBuild = def.DeferBuild

	// validate entry
	if actual.Name == "" {
		return errors.New("name cannot be empty")
	}
	if actual.KeyspaceID == "" {
		return errors.New("keyspace id cannot be empty")
	}
	if len(actual.IndexKeys) == 0 {
		return errors.New("at least one index key must be defined")
	}
	for i, v := range actual.IndexKeys {
		actual.IndexKeys[i] = strings.TrimSpace(v)
		if v == "" {
			return fmt.Errorf("key %d is empty", i)
		}
	}

	// using is the only one we set a default value for
	if actual.Using == "" {
		actual.Using = IndexDefaultUsing
	}

	// lock
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// create as needed
	if dm.defs == nil {
		dm.defs = make(map[string]IndexDefinition)
	}

	// check if index with this name already exists
	if _, ok := dm.defs[actual.Name]; ok {
		return fmt.Errorf("index with name %q already registered", actual.Name)
	}

	// if not found, add
	dm.defs[actual.Name] = *actual

	// keep track of whether we have a deferred build index
	if !dm.hasDeferred && actual.DeferBuild {
		dm.hasDeferred = true
	}

	return nil
}

// Unregister attempts to remove a definition from the map, returning true if something was removed.
func (dm *IndexDefinitionMap) Unregister(name string) bool {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if dm.defs == nil {
		return false
	}
	_, ok := dm.defs[name]
	delete(dm.defs, name)
	return ok
}

// NameList returns a list of all currently registered index definition names
func (dm *IndexDefinitionMap) NameList() []string {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	if dm.defs == nil {
		return make([]string, 0, 0)
	}
	l := len(dm.defs)
	names := make([]string, l, l)
	i := 0
	for n := range dm.defs {
		names[i] = n
		i++
	}
	return names
}

// Map returns all currently registered index definitions
func (dm *IndexDefinitionMap) Map() map[string]IndexDefinition {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	if dm.defs == nil {
		return make(map[string]IndexDefinition, 0)
	}
	tmp := make(map[string]IndexDefinition, len(dm.defs))
	for n, d := range dm.defs {
		tmp[n] = d.clone()
	}
	return tmp
}

// Get attempts to return a specific definition based on name
func (dm *IndexDefinitionMap) Get(name string) (IndexDefinition, bool) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	if dm.defs == nil {
		return IndexDefinition{}, false
	}
	if def, ok := dm.defs[name]; ok {
		return def.clone(), true
	}
	return IndexDefinition{}, false
}

// Clear nils out the map of registered indices.  It is intended to be used after the sync process has finished.
func (dm *IndexDefinitionMap) Clear() {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.defs = nil
}

type ReconcileResults struct {
	InitialCount int      `json:"initial_count"`
	InitialNames []string `json:"initial_names"`

	CreatedCount int      `json:"created_count"`
	CreatedNames []string `json:"created_names"`

	DeletedCount int      `json:"deleted_count"`
	DeletedNames []string `json:"deleted_names"`

	NoopCount int      `json:"noop_count"`
	NoopNames []string `json:"noop_names"`

	FinalCount int      `json:"final_count"`
	FinalNames []string `json:"final_names"`
}

func (r ReconcileResults) String() string {
	return fmt.Sprintf(
		"Reconcile results: initial_count=%d; created_count=%d; deleted_count=%d; noop_count=%d; final_count=%d",
		r.InitialCount,
		r.CreatedCount,
		r.DeletedCount,
		r.NoopCount,
		r.FinalCount,
	)
}

// IndexLocatorFunc is used to retrieve the list of indices of interest from Couchbase
type IndexLocatorFunc func(*gocb.Cluster) (*gocb.QueryResult, error)

// PrefixIndexLocatorFunc constructs a IndexLocatorFunc that queries for a list of indices based on a common prefix
func PrefixIndexLocatorFunc(idxPrefix string, opts *gocb.QueryOptions) IndexLocatorFunc {
	return func(cluster *gocb.Cluster) (*gocb.QueryResult, error) {
		if opts == nil {
			opts = new(gocb.QueryOptions)
			opts.Adhoc = true
			opts.ScanConsistency = gocb.QueryScanConsistencyRequestPlus
			opts.Readonly = true
		}
		return cluster.Query(fmt.Sprintf(DefaultListQueryFormat, idxPrefix), opts)
	}
}

// DecisionFunc is called per index currently present in Couchbase to determine what will happen to it.
type DecisionFunc func(current *IndexDefinition, defMap *IndexDefinitionMap) (IndexAction, error)

// DefaultDecisionFunc attempts to determine if a current index's definition is sane, returning an action appropriate
// to its current state
func DefaultDecisionFunc(current *IndexDefinition, defMap *IndexDefinitionMap) (IndexAction, error) {
	var (
		def IndexDefinition
		ok  bool
	)
	// if current index is not present in map, delete
	if def, ok = defMap.Get(current.Name); !ok {
		return IndexActionDelete, nil
	}
	// if found but it is being forcibly rebuilt or it has major differences, recreate
	if def.ForceRebuild || def.KeyspaceID != current.KeyspaceID || def.Condition != current.Condition {
		return IndexActionRecreate, nil
	}

	// if we get here, more detailed analysis is required...

	// first test to ensure that all expected fields are at least present
KeyOuter:
	for _, key := range def.IndexKeys {
		key = strings.Trim(key, "`")
		for _, currentKey := range current.IndexKeys {
			currentKey = strings.Trim(currentKey, "`")
			if key == currentKey {
				continue KeyOuter
			}
		}
		return IndexActionRecreate, nil
	}

	// if expected fields are present, ensure we don't have extra fields
	if len(def.IndexKeys) != len(current.IndexKeys) {
		return IndexActionRecreate, nil
	}

	// otherwise, probably ok.
	return IndexActionNoop, nil
}

// IndexCreateFunc is called whenever an index is missing from Couchbase
type IndexCreateFunc func(cluster *gocb.Cluster, def IndexDefinition, queryOpts *gocb.QueryOptions) (*gocb.QueryResult, error)

// DefaultIndexCreateFunc is a basic implementation of an IndexCreateFunc that accounts for configurable replicas and
// build deferring
func DefaultIndexCreateFunc(cluster *gocb.Cluster, def IndexDefinition, queryOpts *gocb.QueryOptions) (*gocb.QueryResult, error) {
	// still have to use direct n1ql here as the modeled approach does not allow for num_replica specification
	createQuery := fmt.Sprintf(
		DefaultCreateQueryFormat,
		def.Name,
		def.KeyspaceID,
		strings.Join(def.IndexKeys, ","),
		def.Condition,
		def.Using,
		def.NumReplica,
		def.DeferBuild,
	)
	if queryOpts == nil {
		queryOpts := new(gocb.QueryOptions)
		queryOpts.ScanConsistency = gocb.QueryScanConsistencyRequestPlus
	}
	return cluster.Query(createQuery, queryOpts)
}

// IndexDropFunc is called whenever an index is present in Couchbase that is no longer present in the map provided
// to a Reconciler instance
type IndexDropFunc func(cluster *gocb.Cluster, def IndexDefinition, opts *gocb.DropQueryIndexOptions) error

// DefaultIndexDropFunc is a very basic implementation of an IndexDropFunc that merely calls the upstream gocb/v2
// built-in DropIndex func
func DefaultIndexDropFunc(cluster *gocb.Cluster, def IndexDefinition, opts *gocb.DropQueryIndexOptions) error {
	return cluster.QueryIndexes().DropIndex(def.KeyspaceID, def.Name, opts)
}

// ActionListFinalizerFunc is called after all decisions have been made to allow the user to make any final edits
// before execution
type ActionListFinalizerFunc func(actions []*IndexActionDecision, defMap *IndexDefinitionMap) []*IndexActionDecision

// DefaultActionListFinalizerFunc is a basic implementation of an ActionListFinalizerFunc that merely accepts whatever
// the initial decisions were
func DefaultActionListFinalizerFunc(actions []*IndexActionDecision, _ *IndexDefinitionMap) []*IndexActionDecision {
	return actions
}

type ReconcilerConfig struct {
	// Cluster [required] - Up and valid couchbase cluster connection
	Cluster *gocb.Cluster

	// IndexLocatorFunc [required] - Func used to retrieve list of current indices of interest within Couchbase
	IndexLocatorFunc IndexLocatorFunc

	// DecisionFunc [required] - Func used to determine the action to perform on an index currently within Couchbase
	DecisionFunc DecisionFunc

	// IndexCreateFunc [required] - Func used whenever an index is to be created in Couchbase
	IndexCreateFunc IndexCreateFunc

	// IndexDropFunc [required] - Func used whenever an index is to be dropped from Couchbase
	IndexDropFunc IndexDropFunc

	// ActionListFinalizerFunc [optional] - Optional func to make any final edits to the list of actions to perform on
	// Couchbase before they are executed
	ActionListFinalizerFunc ActionListFinalizerFunc
}

func DefaultReconcilerConfig(cluster *gocb.Cluster, idxLocator IndexLocatorFunc) *ReconcilerConfig {
	conf := new(ReconcilerConfig)
	conf.Cluster = cluster
	conf.IndexLocatorFunc = idxLocator
	conf.DecisionFunc = DefaultDecisionFunc
	conf.IndexCreateFunc = DefaultIndexCreateFunc
	conf.IndexDropFunc = DefaultIndexDropFunc
	conf.ActionListFinalizerFunc = DefaultActionListFinalizerFunc

	return conf
}

type Reconciler struct {
	mu      sync.Mutex
	idxMap  *IndexDefinitionMap
	cluster *gocb.Cluster
	locFn   IndexLocatorFunc

	res *ReconcileResults
}

func NewReconciler(cluster *gocb.Cluster) {

}
