// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (

	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/fatih/color"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"

	"os"
	"path/filepath"
	"sync"
)

const (
	//levelTableFileShortDesc = "Write a tablefile in level order and update manifest"
)

var levelTableFileDocs = cli.CommandDocumentationContent{
	ShortDesc: "Generates tablefile in level order and update manifest",
	LongDesc: `Generate tablefile in level order and update manifest`,
	Synopsis: []string{
		"{{.LessThan}}table name{{.GreaterThan}}",
	},
}

type LevelTableFileCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd LevelTableFileCmd) Name() string {
	return "level-table"
}

// Description returns a description of the command
func (cmd LevelTableFileCmd) Description() string {
	return "Write a tablefile in level order and update manifest."
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
//func (cmd LevelTableFileCmd) RequiresRepo() bool {
//	return false
//}

// Hidden should return true if this command should be hidden from the help text
func (cmd LevelTableFileCmd) Hidden() bool {
	return true
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd LevelTableFileCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	return nil
}

func (cmd LevelTableFileCmd) createArgParser() *argparser.ArgParser {
	return argparser.NewArgParser()
}

// EventType returns the type of the event to log
//func (cmd LevelTableFileCmd) EventType() eventsapi.ClientEventType {
//	return eventsapi.ClientEventType_TYPE_UNSPECIFIED
//}

type CSer interface {
	ChunkStore() chunks.ChunkStore
}

// Exec executes the command
func (cmd LevelTableFileCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, levelTableFileDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() == 0 {
		usage()
		return 1
	}

	var name string
	for _, tableName := range apr.Args() {
		if doltdb.IsReadOnlySystemTable(tableName) {
			return HandleVErrAndExitCode(
				errhand.BuildDError("unable to do stuff on system table %s", tableName).AddCause(doltdb.ErrSystemTableCannotBeModified).Build(), usage)
		}
		_, err := fmt.Fprintf(color.Error, "gonna do stuff with table: %s\n", tableName)
		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("printing prob").AddCause(err).Build(), usage)
		}
		name = tableName
	}

	if name == "" {
		return HandleVErrAndExitCode(errhand.BuildDError("name must be defined na").Build(), usage)
	}

	// todo: handle multienv

	//mrEnv := env.DoltEnvAsMultiEnv(dEnv)
	////if err != nil {
	////	return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	////}
	//
	//roots, err := mrEnv.GetWorkingRoots(ctx)
	//if err != nil {
	//	return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	//}
	//
	//if len(roots) != 1 {
	//	return HandleVErrAndExitCode(errhand.BuildDError("only one root right na").Build(), usage)
	//}
	//if roots == nil {
	//	return HandleVErrAndExitCode(errhand.BuildDError("roots cant be nil na").Build(), usage)
	//}

	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		panic(err)
	}

	t, _, err := root.GetTable(ctx, name)
	if err != nil {
		panic(err)
	}

	if t == nil {
		panic(fmt.Sprintf("expected %s table", name))
	}

	rd, err := t.GetRowData(ctx)
	if err != nil {
		panic(err)
	}

	hashes := types.DebugVisitMap(rd)

	_, err = fmt.Fprintf(color.Error, "found fucking hashes of length: %d\n", len(hashes))
	if err != nil {
		panic(err)
	}

	if len(hashes) < 1 {
		_, err := fmt.Fprintf(color.Output, "table is only leaf nodes, nothing to do bitch\n")
		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError(err.Error()).Build(), usage)
		}
		return HandleVErrAndExitCode(nil, usage)
	}

	if true {
		cs := root.VRW().(CSer).ChunkStore()

		hashset := hash.HashSet{}
		for _, h := range hashes {
			hashset.Insert(hash.Parse(h))
		}

		byHash := make(map[string]nbs.CompressedChunk)
		mu := new(sync.Mutex)
		err = cs.(*nbs.NBSMetricWrapper).GetManyCompressed(ctx, hashset, func(cc nbs.CompressedChunk) {
			mu.Lock()
			byHash[cc.Hash().String()] = cc
			mu.Unlock()
		})
		if err != nil {
			panic(err)
		}

		cwd, err := os.Getwd()
		if err != nil {
			panic(err)
		}

		writer, err := nbs.NewCmpChunkTableWriter(cwd)
		if err != nil {
			panic(err)
		}
		for _, h := range hashes {
			err = writer.AddCmpChunk(byHash[h])
			if err != nil {
				panic(err)
			}
		}

		filename, err := writer.Finish()
		if err != nil {
			panic(err)
		}

		newTableFile := filepath.Join(cwd, filename)
		err = writer.FlushToFile(newTableFile)
		if err != nil {
			panic(err)
		}
		_, err = fmt.Fprintf(color.Error, "ChunkCount(): %d\n", writer.ChunkCount())
		if err != nil {
			panic(err)
		}

		err = os.Rename(newTableFile, filepath.Join(dbfactory.DoltDataDir, filename))
		if err != nil {
			panic(err)
		}

		//ts, ok := cs.(nbs.TableFileStore)
		//if !ok {
		//	panic("bootleg cs is not a Table File Store")
		//}
		//size, err := ts.Size(ctx)
		//if err != nil {
		//	panic(err)
		//}
		//if size == 0 {
		//	panic("needs a size bro!")
		//}
	}

	return HandleVErrAndExitCode(nil, usage)
}
