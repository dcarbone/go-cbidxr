# go-cbidxr
Couchbase index reconciliation tools for golang

[![](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://godoc.org/github.com/dcarbone/go-cbidxr)

## Concept
This is, in effect, a state comparison library.  You are expected to construct the desire state of indices, which are 
then compared against what is actually present in the connected Couchbase cluster, and differences are reconciled.

Basic order of operations:

1. Define desired index state
1. Connect to Couchbase cluster
1. Perform action decision logic
1. Execute non-noop actions

## 1. Define desire index state
Your desired state is defined by creating one or more [IndexDefinition](https://godoc.org/github.com/dcarbone/go-cbidxr#IndexDefinition)
type(s).  The models within support being defined from JSON or HCL.

As an example:

```go
package main

import (
    "github.com/dcarbone/go-cbidxr"
)

const (
    indexPrefix = "my-app-"
    bucketName = "mybucket"
)

var (
    indexDefinitions = []*cbidxr.IndexDefinition{
        {
            Name:         indexPrefix + "idx1",
            KeyspaceID:   bucketName,
            IndexKey:     []string{"`field1`", "`field2`"},
            Condition:    "(`_type` = \"sandwiches\")",
            Using:        cbidxr.IndexDefaultUsing,
            NumReplica:   0,
            DeferBuild:   true,
            ForceRebuild: false,
        },
        {
            Name:         indexPrefix + "idx2",
            KeyspaceID:   bucketName,
            IndexKey:     []string{"`field3`", "`field4`"},
            Condition:    "(`_type` != \"sandwiches\")",
            Using:        cbidxr.IndexDefaultUsing,
            NumReplica:   0,
            DeferBuild:   true,
            ForceRebuild: false,
        },
    }
)
```

You can, obviously, specify as many indices as you need.  Given the drastic improvement in n1ql performance, I suggest
using quite a few.

## 2. Connect to Couchbase cluster

This library is built on top of [gocb/v2](https://pkg.go.dev/github.com/couchbase/gocb/v2).

Follow its documentation for how to connect to your cluster

## 3. Perform action decision logic

As part of constructing a [Reconciler](https://godoc.org/github.com/dcarbone/go-cbidxr#Reconciler), you must first
construct a [ReconcilerConfig](https://godoc.org/github.com/dcarbone/go-cbidxr#ReconcilerConfig) type.

This type specifies a few key fields:

#### IndexLocatorFunc
Called to build the list of indices currently within Couchbase that you are interested in comparing your
desired state against.

Provided implementations:
* [PrefixIndexLocatorFunc](https://godoc.org/github.com/dcarbone/go-cbidxr#PrefixIndexLocatorFunc) - Searches for
indices with a specific prefix (i.e. `"my-app-%"`)

#### DecisionFunc
Called per current index that is found.

Provided implementations:
*  [DefaultDecisionFunc](https://godoc.org/github.com/dcarbone/go-cbidxr#DefaultDecisionFunc) - Performs a basic
comparison to determine if the current index must be dropped / created / is equivalent to the desired state.

#### IndexCreateFunc
Called when an index in the desired state is not present or as the 2nd operation in a "Recreate" 
action

Provided implementations:
* [DefaultIndexCreateFunc](https://godoc.org/github.com/dcarbone/go-cbidxr#DefaultIndexCreateFunc) - Executes simple
create n1ql query with support for build deferring and `num_replica` value setting.

#### IndexDropFunc
Called when an index currently exists within Couchbase that is not in the desired state or as the
first task during a "Recreate" action.

Provided implementations:
* [DefaultIndexDropFunc](https://godoc.org/github.com/dcarbone/go-cbidxr#DefaultIndexDropFunc) - Executes the built-in
gocb/v2 `DropIndex()` method.

#### ActionListFinalizerFunc
Called once initial list of action decisions have been made, allowing you to make any final modifications before they 
are acted upon

Provided implementations:
* [DefaultActionListFinalizerFunc](https://godoc.org/github.com/dcarbone/go-cbidxr#DefaultActionListFinalizerFunc) -
Makes no modifications to the computed action list

## 4. Execute non-noop actions
Once the list of actions have been determined, any decision with an [IndexAction](https://godoc.org/github.com/dcarbone/go-cbidxr#IndexAction)
other than `IndexActionNoop` are acted upon.