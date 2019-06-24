// Copyright 2018 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"

	"github.com/attic-labs/kingpin"

	"github.com/liquidata-inc/ld/dolt/go/store/cmd/noms/util"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/diff"
	"github.com/liquidata-inc/ld/dolt/go/store/spec"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

func nomsList(ctx context.Context, noms *kingpin.Application) (*kingpin.CmdClause, util.KingpinHandler) {
	list := noms.Command("list", "interact with lists")

	listNew := list.Command("new", "creates a new list")
	newDb := listNew.Arg("database", "spec to db to create list within").Required().String()
	newEntries := listNew.Arg("items", "items to insert").Strings()

	listAppend := list.Command("append", "appends one or more items to a list")
	appendSpec := listAppend.Arg("spec", "value spec for the list to edit").Required().String()
	appendEntries := listAppend.Arg("items", "items to insert").Strings()

	listInsert := list.Command("insert", "inserts one or more items into a list")
	insertAt := listInsert.Arg("pos", "position to insert new items at").Required().Uint64()
	insertSpec := listInsert.Arg("spec", "value spec for the list to edit").Required().String()
	insertEntries := listInsert.Arg("items", "items to insert").Strings()

	listDel := list.Command("del", "removes one or more items from the list")
	delSpec := listDel.Arg("spec", "value spec for the list to edit").Required().String()
	delPos := listDel.Arg("pos", "index to remove items at").Required().Uint64()
	delLen := listDel.Arg("len", "number of items to remove").Required().Uint64()

	return list, func(input string) int {
		switch input {
		case listNew.FullCommand():
			return nomsListNew(ctx, *newDb, *newEntries)
		case listAppend.FullCommand():
			return nomsListAppend(ctx, *appendSpec, *appendEntries)
		case listInsert.FullCommand():
			return nomsListInsert(ctx, *insertSpec, *insertAt, *insertEntries)
		case listDel.FullCommand():
			return nomsListDel(ctx, *delSpec, *delPos, *delLen)
		}
		d.Panic("notreached")
		return 1
	}
}

func nomsListNew(ctx context.Context, dbStr string, args []string) int {
	sp, err := spec.ForDatabase(dbStr)
	d.PanicIfError(err)
	applyListInserts(ctx, sp, types.NewList(ctx, sp.GetDatabase(ctx)), nil, 0, args)
	return 0
}

func nomsListAppend(ctx context.Context, specStr string, args []string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)
	rootVal, basePath := splitPath(ctx, sp)
	if list, ok := rootVal.(types.List); ok {
		applyListInserts(ctx, sp, rootVal, basePath, list.Len(), args)
	} else {
		util.CheckErrorNoUsage(fmt.Errorf("%s is not a list", specStr))
	}
	return 0
}

func nomsListInsert(ctx context.Context, specStr string, pos uint64, args []string) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)
	rootVal, basePath := splitPath(ctx, sp)
	applyListInserts(ctx, sp, rootVal, basePath, pos, args)
	return 0
}

func nomsListDel(ctx context.Context, specStr string, pos uint64, len uint64) int {
	sp, err := spec.ForPath(specStr)
	d.PanicIfError(err)

	rootVal, basePath := splitPath(ctx, sp)
	patch := diff.Patch{}
	// TODO: if len-pos is large this will start to become problematic
	for i := pos; i < pos+len; i++ {
		patch = append(patch, diff.Difference{
			Path:       append(basePath, types.NewIndexPath(types.Float(i))),
			ChangeType: types.DiffChangeRemoved,
		})
	}

	appplyPatch(ctx, sp, rootVal, basePath, patch)
	return 0
}

func applyListInserts(ctx context.Context, sp spec.Spec, rootVal types.Value, basePath types.Path, pos uint64, args []string) {
	if rootVal == nil {
		util.CheckErrorNoUsage(fmt.Errorf("No value at: %s", sp.String()))
		return
	}
	db := sp.GetDatabase(ctx)
	patch := diff.Patch{}
	for i := 0; i < len(args); i++ {
		vv, err := argumentToValue(ctx, args[i], db)
		if err != nil {
			util.CheckError(fmt.Errorf("Invalid value: %s at position %d: %s", args[i], i, err))
		}
		patch = append(patch, diff.Difference{
			Path:       append(basePath, types.NewIndexPath(types.Float(pos+uint64(i)))),
			ChangeType: types.DiffChangeAdded,
			NewValue:   vv,
		})
	}
	appplyPatch(ctx, sp, rootVal, basePath, patch)
}
