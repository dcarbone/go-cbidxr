package cbidxr

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/couchbase/gocb/v2"
)

const (
	IndexDefaultUsing = "gsi"

	DefaultListQueryFormat   = "SELECT `indexes`.* FROM system:indexes WHERE `name` LIKE \"%s%%\""
	DefaultCreateQueryFormat = "CREATE INDEX `%s` ON `%s`(%s) WHERE (%s) USING %s WITH {\"num_replica\": %d, \"defer_build\": %t}"
)

type IndexAction string

const (
	IndexActionNoop     IndexAction = "noop"
	IndexActionDrop     IndexAction = "drop"
	IndexActionRecreate IndexAction = "recreate"
	IndexActionCreate   IndexAction = "create"
)

const (
	IndexActionReasonNoop = "Index definitions match"

	IndexActionReasonDropExcess = "Index is not in desired state map"

	IndexActionReasonRecreateForced             = "Index set to forcibly rebuild"
	IndexActionReasonRecreateKeyspaceIDMismatch = "Index has mismatched KeyspaceID: current=%q; expected=%q"
	IndexActionReasonRecreateConditionMismatch  = "Index has mismatched Condition: current=%q; expected=%q"
	IndexActionReasonRecreateIndexKeysMismatch  = "Index has mismatched IndexKey: current=%v; expected=%v"

	IndexActionReasonCreate = "Index is missing from Couchbase"
)

// IndexActionDecision describes the outcome of the DecisionFunc
type IndexActionDecision struct {
	// IndexName is the name of the index being acted upon
	IndexName string `json:"name"`

	// Action describes what will happen to this index
	Action IndexAction `json:"action"`

	// ActionReason should contain a descriptive reason for this decision
	ActionReason string `json:"action_reason"`

	// CurrentDefinition will be populated if an index with this name already exists within Couchbase
	CurrentDefinition *IndexDefinition `json:"current_definition"`

	// NewDefinition will be populated if an index with this name is present in the new DefinitionMap provided to the
	// parent Reconciler
	NewDefinition *IndexDefinition `json:"new_definition"`
}

// IndexDefinition is the representation of both current and desired indices
type IndexDefinition struct {
	// Name [required] - The of the index to create / match
	Name string `json:"name" hcl:"name"`

	// KeyspaceID [required] - The name of the bucket to attach this index to
	KeyspaceID string `json:"keyspace_id" hcl:"keyspace_id"`

	// IndexKey [suggested] - List of keys within document objects to used to build this index.  You are
	// responsible for escaping, i.e. []string{"`field1`", "`obj1`.`field2`"}
	IndexKey []string `json:"index_key" hcl:"index_key"`

	// Condition [optional] - An optional WHERE clause for this index
	Condition string `json:"condition" hcl:"condition"`

	// Using [optional] - Generally probably want GSI
	Using string `json:"using" hcl:"using"`

	// NumReplica [optional] - Number of index replicas to create.  Only available to Couchbase Enterprise customers.
	NumReplica uint `json:"num_replica" hcl:"num_replica"`

	// DeferBuild [suggested] - This effectively speeds up execution as all indices will not be populated until the end
	DeferBuild bool `json:"defer_build" hcl:"defer_build"`

	// ForceRebuild [optional] - Set to true if you want the decision to automatically be "recreate" for indices that
	// currently exist
	ForceRebuild bool `json:"force_rebuild" hcl:"force_rebuild"`
}

func (def *IndexDefinition) clone() IndexDefinition {
	if def == nil {
		return IndexDefinition{}
	}
	tmp := new(IndexDefinition)
	*tmp = *def
	tmp.IndexKey = make([]string, len(def.IndexKey))
	copy(tmp.IndexKey, def.IndexKey)
	return *tmp
}

func (def *IndexDefinition) clonePtr() *IndexDefinition {
	cloned := def.clone()
	return &cloned
}

type IndexDefinitionMap struct {
	mu          sync.RWMutex
	defs        map[string]IndexDefinition
	hasDeferred bool
}

func NewIndexDefinitionMap(defs ...*IndexDefinition) (*IndexDefinitionMap, error) {
	dm := new(IndexDefinitionMap)
	if err := dm.Register(defs...); err != nil {
		return nil, err
	}
	return dm, nil
}

func (dm *IndexDefinitionMap) doRegister(def *IndexDefinition) error {
	if def == nil {
		return errors.New("nil index definition provided")
	}
	// build actual entry
	actual := new(IndexDefinition)
	actual.Name = strings.TrimSpace(def.Name)
	actual.KeyspaceID = strings.TrimSpace(def.KeyspaceID)
	actual.IndexKey = make([]string, len(def.IndexKey))
	copy(actual.IndexKey, def.IndexKey)
	actual.Condition = strings.TrimSpace(def.Condition)
	actual.Using = strings.TrimSpace(def.Using)
	actual.NumReplica = def.NumReplica
	actual.DeferBuild = def.DeferBuild
	actual.ForceRebuild = def.ForceRebuild

	// validate entry
	if actual.Name == "" {
		return errors.New("name cannot be empty")
	}
	if actual.KeyspaceID == "" {
		return errors.New("keyspace id cannot be empty")
	}
	if len(actual.IndexKey) == 0 {
		return errors.New("at least one index key must be defined")
	}
	for i, v := range actual.IndexKey {
		actual.IndexKey[i] = strings.TrimSpace(v)
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

// Register validates and then adds the provided definition(s) the map
func (dm *IndexDefinitionMap) Register(defs ...*IndexDefinition) error {
	for _, def := range defs {
		if err := dm.doRegister(def); err != nil {
			return err
		}
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

// IndexLocatorFunc is used to retrieve the list of indices of interest from Couchbase
type IndexLocatorFunc func(*gocb.Cluster) ([]*IndexDefinition, error)

// PrefixIndexLocatorFunc constructs a IndexLocatorFunc that queries for a list of indices based on a common prefix
func PrefixIndexLocatorFunc(idxPrefix string, opts *gocb.QueryOptions) IndexLocatorFunc {
	return func(cluster *gocb.Cluster) ([]*IndexDefinition, error) {
		if opts == nil {
			opts = new(gocb.QueryOptions)
			opts.Adhoc = true
			opts.ScanConsistency = gocb.QueryScanConsistencyRequestPlus
			opts.Readonly = true
		}
		res, err := cluster.Query(fmt.Sprintf(DefaultListQueryFormat, idxPrefix), opts)
		if err != nil {
			return nil, err
		}
		current := make([]*IndexDefinition, 0)
		for res.Next() {
			idxDef := new(IndexDefinition)
			if err := res.Row(idxDef); err != nil {
				return nil, fmt.Errorf("error fetching row from index locator query: %v", err)
			}
			current = append(current, idxDef)
		}
		return current, nil
	}
}

// DecisionFunc is called per index currently present in Couchbase to determine what will happen to it.
type DecisionFunc func(current *IndexDefinition, defMap *IndexDefinitionMap) (*IndexActionDecision, error)

// DefaultDecisionFunc attempts to determine if a current index's definition is sane, returning an action appropriate
// to its current state
func DefaultDecisionFunc(current *IndexDefinition, defMap *IndexDefinitionMap) (*IndexActionDecision, error) {
	var (
		newDef IndexDefinition
		ok     bool

		decision = new(IndexActionDecision)
	)

	decision.IndexName = current.Name
	decision.CurrentDefinition = current

	// if current index is not present in map, delete
	if newDef, ok = defMap.Get(current.Name); !ok {
		decision.Action = IndexActionDrop
		decision.ActionReason = IndexActionReasonDropExcess
		return decision, nil
	}

	// set updated def to decision
	decision.NewDefinition = newDef.clonePtr()

	// if found but it is being forcibly rebuilt or it has major differences, recreate
	if newDef.ForceRebuild {
		decision.Action = IndexActionRecreate
		decision.ActionReason = IndexActionReasonRecreateForced
		return decision, nil
	}
	if newDef.KeyspaceID != current.KeyspaceID {
		decision.Action = IndexActionRecreate
		decision.ActionReason = fmt.Sprintf(IndexActionReasonRecreateKeyspaceIDMismatch, current.KeyspaceID, newDef.KeyspaceID)
		return decision, nil
	}
	if newDef.Condition != current.Condition {
		decision.Action = IndexActionRecreate
		decision.ActionReason = fmt.Sprintf(IndexActionReasonRecreateConditionMismatch, current.Condition, newDef.Condition)
		return decision, nil
	}

	// if we get here, more detailed analysis is required...

	// first test to ensure that all expected fields are at least present
KeyOuter:
	for _, key := range newDef.IndexKey {
		key = strings.Trim(key, "`")
		for _, currentKey := range current.IndexKey {
			currentKey = strings.Trim(currentKey, "`")
			if key == currentKey {
				continue KeyOuter
			}
		}
		decision.Action = IndexActionRecreate
		decision.ActionReason = fmt.Sprintf(IndexActionReasonRecreateIndexKeysMismatch, current.IndexKey, newDef.IndexKey)
		return decision, nil
	}

	// if expected fields are present, ensure we don't have extra fields
	if len(newDef.IndexKey) != len(current.IndexKey) {
		decision.Action = IndexActionRecreate
		decision.ActionReason = fmt.Sprintf(IndexActionReasonRecreateIndexKeysMismatch, current.IndexKey, newDef.IndexKey)
		return decision, nil
	}

	// otherwise, probably ok.
	decision.Action = IndexActionNoop
	decision.ActionReason = IndexActionReasonNoop
	return decision, nil
}

// IndexCreateFunc is called whenever an index is missing from Couchbase
type IndexCreateFunc func(cluster *gocb.Cluster, def IndexDefinition) (*gocb.QueryResult, error)

// DefaultIndexCreateFunc is a basic implementation of an IndexCreateFunc that accounts for configurable replicas and
// build deferring
func DefaultIndexCreateFunc(cluster *gocb.Cluster, def IndexDefinition) (*gocb.QueryResult, error) {
	// still have to use direct n1ql here as the modeled approach does not allow for num_replica specification
	createQuery := fmt.Sprintf(
		DefaultCreateQueryFormat,
		def.Name,
		def.KeyspaceID,
		strings.Join(def.IndexKey, ","),
		def.Condition,
		def.Using,
		def.NumReplica,
		def.DeferBuild,
	)
	queryOpts := new(gocb.QueryOptions)
	queryOpts.ScanConsistency = gocb.QueryScanConsistencyRequestPlus
	return cluster.Query(createQuery, queryOpts)
}

// IndexDropFunc is called whenever an index is present in Couchbase that is no longer present in the map provided
// to a Reconciler instance
type IndexDropFunc func(cluster *gocb.Cluster, def IndexDefinition) error

// DefaultIndexDropFunc is a very basic implementation of an IndexDropFunc that merely calls the upstream gocb/v2
// built-in DropIndex func
func DefaultIndexDropFunc(cluster *gocb.Cluster, def IndexDefinition) error {
	opts := new(gocb.DropQueryIndexOptions)
	opts.IgnoreIfNotExists = true
	return cluster.QueryIndexes().DropIndex(def.KeyspaceID, def.Name, nil)
}

// ActionListFinalizerFunc is called after all decisions have been made to allow the user to make any final edits
// before execution
type ActionListFinalizerFunc func(actions []*IndexActionDecision, defMap *IndexDefinitionMap) ([]*IndexActionDecision, error)

// DefaultActionListFinalizerFunc is a basic implementation of an ActionListFinalizerFunc that merely accepts whatever
// the initial decisions were
func DefaultActionListFinalizerFunc(actions []*IndexActionDecision, _ *IndexDefinitionMap) ([]*IndexActionDecision, error) {
	return actions, nil
}

type ReconcileResults struct {
	InitialCount int      `json:"initial_count"`
	InitialNames []string `json:"initial_names"`

	CreatedCount int      `json:"created_count"`
	CreatedNames []string `json:"created_names"`

	DroppedCount int      `json:"dropped_count"`
	DroppedNames []string `json:"dropped_names"`

	NoopCount int      `json:"noop_count"`
	NoopNames []string `json:"noop_names"`

	FinalCount int      `json:"final_count"`
	FinalNames []string `json:"final_names"`

	ActionList []*IndexActionDecision `json:"action_list"`

	Err error `json:"err"`
}

func (r ReconcileResults) String() string {
	return fmt.Sprintf(
		"Reconcile results: initial_count=%d; created_count=%d; deleted_count=%d; noop_count=%d; final_count=%d",
		r.InitialCount,
		r.CreatedCount,
		r.DroppedCount,
		r.NoopCount,
		r.FinalCount,
	)
}

func (r ReconcileResults) Error() string {
	if r.Err == nil {
		return ""
	}
	return r.Err.Error()
}

func newReconcileResults() *ReconcileResults {
	rr := new(ReconcileResults)
	rr.InitialNames = make([]string, 0)
	rr.CreatedNames = make([]string, 0)
	rr.DroppedNames = make([]string, 0)
	rr.NoopNames = make([]string, 0)
	rr.FinalNames = make([]string, 0)
	rr.ActionList = make([]*IndexActionDecision, 0)
	return rr
}

type ReconcilerConfig struct {
	// Cluster [required] - Up and valid couchbase cluster connection
	Cluster *gocb.Cluster

	// IndexDefinitionMap [optional] - Optional index map to provide at time of construction.  If left empty, a new
	// instance will be created as part of creating a new Reconciler
	IndexDefinitionMap *IndexDefinitionMap

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

	// Logger [optional] - If you want logging, define this.
	Logger *log.Logger

	// Debug [optional] - Set to true for debug-level logging
	Debug bool
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

// Reconciler is a Couchbase bucket index management utility that enables developers to control their state in code.
//
// No concurrency protection is done, control yourself.
type Reconciler struct {
	log *log.Logger
	dbg bool

	cluster *gocb.Cluster

	idxMap *IndexDefinitionMap

	locFn       IndexLocatorFunc
	decFn       DecisionFunc
	idxCreateFn IndexCreateFunc
	idxDropFn   IndexDropFunc
	finalizerFn ActionListFinalizerFunc

	res *ReconcileResults
}

// NewReconciler creates a new Reconciler instance based on the provided config
func NewReconciler(conf *ReconcilerConfig) (*Reconciler, error) {
	rc := new(Reconciler)
	rc.log = conf.Logger
	rc.dbg = conf.Debug

	rc.cluster = conf.Cluster
	rc.locFn = conf.IndexLocatorFunc

	if conf.IndexDefinitionMap == nil {
		rc.idxMap, _ = NewIndexDefinitionMap()
	} else {
		rc.idxMap = conf.IndexDefinitionMap
	}

	def := DefaultReconcilerConfig(conf.Cluster, conf.IndexLocatorFunc)
	if conf.DecisionFunc != nil {
		rc.decFn = conf.DecisionFunc
	} else {
		rc.decFn = def.DecisionFunc
	}
	if conf.IndexCreateFunc != nil {
		rc.idxCreateFn = conf.IndexCreateFunc
	} else {
		rc.idxCreateFn = def.IndexCreateFunc
	}
	if conf.IndexDropFunc != nil {
		rc.idxDropFn = conf.IndexDropFunc
	} else {
		rc.idxDropFn = def.IndexDropFunc
	}
	if conf.ActionListFinalizerFunc != nil {
		rc.finalizerFn = conf.ActionListFinalizerFunc
	} else {
		rc.finalizerFn = def.ActionListFinalizerFunc
	}

	return rc, nil
}

func (rc *Reconciler) logf(debug bool, f string, v ...interface{}) {
	if rc.log == nil || (debug && !rc.dbg) {
		return
	}
	rc.log.Printf(f, v...)
}

func (rc *Reconciler) Definitions() *IndexDefinitionMap {
	return rc.idxMap
}

func (rc *Reconciler) RegisterDefinitions(defs ...*IndexDefinition) error {
	rc.logf(true, "Registering %d definitions...", len(defs))
	return rc.idxMap.Register(defs...)
}

func (rc *Reconciler) Execute() (*ReconcileResults, error) {
	deferBuildMap := make(map[string]struct{})

	rc.logf(false, "Reconciling indices...")

	recResults := newReconcileResults()
	actDecisions := make([]*IndexActionDecision, 0)
	// todo: this is a very lazy way to do this.
	currentNameMap := make(map[string]struct{})

	// get current index list
	currentIndices, err := rc.locFn(rc.cluster)
	if err != nil {
		recResults.Err = err
		return recResults, recResults.Err
	}

	// update results
	recResults.InitialCount = len(currentIndices)
	for _, curr := range currentIndices {
		recResults.InitialNames = append(recResults.InitialNames, curr.Name)
	}

	rc.logf(true, "Found indices: %d (%v)", recResults.InitialCount, recResults.InitialNames)

	// determine what to do with current definitions
	for _, curr := range currentIndices {
		currentNameMap[curr.Name] = struct{}{}
		if decision, err := rc.decFn(curr, rc.idxMap); err != nil {
			recResults.Err = fmt.Errorf("error executing %T on index %q: %v", rc.decFn, curr.Name, err)
			return recResults, recResults.Err
		} else {
			rc.logf(true, "Decision for index %q: %v", curr.Name, decision)
			actDecisions = append(actDecisions, decision)
		}
	}

	// loop through desired index state map
	for name, upd := range rc.idxMap.Map() {
		// keep track of which buckets have indexes with deferred builds on them.
		if upd.DeferBuild {
			if _, ok := deferBuildMap[upd.KeyspaceID]; !ok {
				rc.logf(true, "Adding bucket %q to list with deferred build indices", upd.Name)
				deferBuildMap[upd.KeyspaceID] = struct{}{}
			}
		}
		// if registered here, decision was made in previous loop
		if _, ok := currentNameMap[name]; ok {
			continue
		}
		// index is missing, add create action
		decision := &IndexActionDecision{
			IndexName:     upd.Name,
			Action:        IndexActionCreate,
			ActionReason:  IndexActionReasonCreate,
			NewDefinition: upd.clonePtr(),
		}
		actDecisions = append(actDecisions, decision)
		rc.logf(true, "Decision for index %q: %v", upd.Name, decision)
	}

	// finalize the action list
	finalActionList, err := rc.finalizerFn(actDecisions, rc.idxMap)
	if err != nil {
		recResults.Err = fmt.Errorf("error finalizing action list: %w", err)
		return recResults, recResults.Err
	}

	for _, act := range finalActionList {
		switch act.Action {
		case IndexActionNoop:
			rc.logf(true, "No actions required for %q", act.IndexName)
			recResults.NoopCount++
			recResults.NoopNames = append(recResults.NoopNames, act.IndexName)
			recResults.FinalCount++
			recResults.FinalNames = append(recResults.FinalNames, act.IndexName)
		case IndexActionCreate:
			rc.logf(true, "Creating index %q...", act.IndexName)
			if _, err := rc.idxCreateFn(rc.cluster, *act.NewDefinition); err != nil {
				recResults.Err = fmt.Errorf("error executing index create func %T on index %q: %w", rc.idxCreateFn, act.IndexName, err)
				return recResults, recResults.Err
			}
			recResults.CreatedCount++
			recResults.CreatedNames = append(recResults.CreatedNames, act.IndexName)
			recResults.FinalCount++
			recResults.FinalNames = append(recResults.FinalNames, act.IndexName)
		case IndexActionDrop:
			rc.logf(true, "Dropping index %q...", act.IndexName)
			if err := rc.idxDropFn(rc.cluster, *act.CurrentDefinition); err != nil {
				recResults.Err = fmt.Errorf("error executing index drop func %T on index %q: %w", rc.idxDropFn, act.IndexName, err)
				return recResults, recResults.Err
			}
			recResults.DroppedCount++
			recResults.DroppedNames = append(recResults.DroppedNames, act.IndexName)
		case IndexActionRecreate:
			rc.logf(true, "Recreating index %q...", act.IndexName)
			if err := rc.idxDropFn(rc.cluster, *act.CurrentDefinition); err != nil {
				recResults.Err = fmt.Errorf("error executing index drop func %T on index %q: %w", rc.idxDropFn, act.IndexName, err)
				return recResults, recResults.Err
			}
			recResults.DroppedCount++
			recResults.DroppedNames = append(recResults.DroppedNames, act.IndexName)
			if _, err := rc.idxCreateFn(rc.cluster, *act.NewDefinition); err != nil {
				recResults.Err = fmt.Errorf("error executing index create func %T on index %q: %w", rc.idxCreateFn, act.IndexName, err)
				return recResults, recResults.Err
			}
			recResults.CreatedCount++
			recResults.CreatedNames = append(recResults.CreatedNames, act.IndexName)
			recResults.FinalCount++
			recResults.FinalNames = append(recResults.FinalNames, act.IndexName)

		default:
			panic(fmt.Sprintf("Unknown action %q seen", act.Action))
		}
	}

	if l := len(deferBuildMap); l > 0 {
		for bn := range deferBuildMap {
			rc.logf(true, "Building deferred indices on bucket %q...", bn)
			if _, err := rc.cluster.QueryIndexes().BuildDeferredIndexes(bn, nil); err != nil {
				recResults.Err = fmt.Errorf("error building deferred indices on bucket %q: %w", bn, err)
				return recResults, recResults.Err
			}
		}
	} else {
		rc.logf(true, "No deferred build indices found")
	}

	recResults.ActionList = finalActionList

	rc.logf(true, "Reconciliation completed")

	return recResults, nil
}
