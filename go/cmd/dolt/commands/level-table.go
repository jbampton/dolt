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
	return "level-tablefile"
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

	//nbs.DebugGetLocations = false
	hashes := types.DebugVisitMap(rd)

	_, err = fmt.Fprintf(color.Error, "found fucking hashes of length: %d\n", len(hashes))
	if err != nil {
		panic(err)
	}

	if true {
		cs := root.VRW().(CSer).ChunkStore()
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
		fmt.Fprintf(color.Error, "ChunkCount(): %d\n", writer.ChunkCount())
	}

	return HandleVErrAndExitCode(nil, usage)
}

//func parseArgs(apr *argparser.ArgParseResults) (string, string, errhand.VerboseError) {
//	if apr.NArg() < 1 || apr.NArg() > 2 {
//		return "", "", errhand.BuildDError("").SetPrintUsage().Build()
//	}
//
//	urlStr := apr.Arg(0)
//	_, err := earl.Parse(urlStr)
//
//	if err != nil {
//		return "", "", errhand.BuildDError("error: invalid remote url: " + urlStr).Build()
//	}
//
//	var dir string
//	if apr.NArg() == 2 {
//		dir = apr.Arg(1)
//	} else {
//		dir = path.Base(urlStr)
//		if dir == "." {
//			dir = path.Dir(urlStr)
//		} else if dir == "/" {
//			return "", "", errhand.BuildDError("Could not infer repo name.  Please explicitily define a directory for this url").Build()
//		}
//	}
//
//	return dir, urlStr, nil
//}
//
//func envForClone(ctx context.Context, nbf *types.NomsBinFormat, r env.Remote, dir string, fs filesys.Filesys, version string) (*env.DoltEnv, errhand.VerboseError) {
//	exists, _ := fs.Exists(filepath.Join(dir, dbfactory.DoltDir))
//
//	if exists {
//		return nil, errhand.BuildDError("error: data repository already exists at " + dir).Build()
//	}
//
//	err := fs.MkDirs(dir)
//
//	if err != nil {
//		return nil, errhand.BuildDError("error: unable to create directories: " + dir).Build()
//	}
//
//	err = os.Chdir(dir)
//
//	if err != nil {
//		return nil, errhand.BuildDError("error: unable to access directory " + dir).Build()
//	}
//
//	dEnv := env.Load(ctx, env.GetCurrentUserHomeDir, fs, doltdb.LocalDirDoltDB, version)
//	err = dEnv.InitRepoWithNoData(ctx, nbf)
//
//	if err != nil {
//		return nil, errhand.BuildDError("error: unable to initialize repo without data").AddCause(err).Build()
//	}
//
//	dEnv.RSLoadErr = nil
//	if !env.IsEmptyRemote(r) {
//		dEnv.RepoState, err = env.CloneRepoState(dEnv.FS, r)
//
//		if err != nil {
//			return nil, errhand.BuildDError("error: unable to create repo state with remote " + r.Name).AddCause(err).Build()
//		}
//	}
//
//	return dEnv, nil
//}
//
//func createRemote(ctx context.Context, remoteName, remoteUrl string, params map[string]string) (env.Remote, *doltdb.DoltDB, errhand.VerboseError) {
//	cli.Printf("cloning %s\n", remoteUrl)
//
//	r := env.NewRemote(remoteName, remoteUrl, params)
//
//	ddb, err := r.GetRemoteDB(ctx, types.Format_Default)
//
//	if err != nil {
//		bdr := errhand.BuildDError("error: failed to get remote db").AddCause(err)
//
//		if err == remotestorage.ErrInvalidDoltSpecPath {
//			urlObj, _ := earl.Parse(remoteUrl)
//			bdr.AddDetails("'%s' should be in the format 'organization/repo'", urlObj.Path)
//		}
//
//		return env.NoRemote, nil, bdr.Build()
//	}
//
//	return r, ddb, nil
//}
//
//func cloneProg(eventCh <-chan datas.TableFileEvent) {
//	var (
//		chunks            int64
//		chunksDownloading int64
//		chunksDownloaded  int64
//		cliPos            int
//	)
//
//	cliPos = cli.DeleteAndPrint(cliPos, "Retrieving remote information.")
//	for tblFEvt := range eventCh {
//		switch tblFEvt.EventType {
//		case datas.Listed:
//			for _, tf := range tblFEvt.TableFiles {
//				chunks += int64(tf.NumChunks())
//			}
//		case datas.DownloadStart:
//			for _, tf := range tblFEvt.TableFiles {
//				chunksDownloading += int64(tf.NumChunks())
//			}
//		case datas.DownloadSuccess:
//			for _, tf := range tblFEvt.TableFiles {
//				chunksDownloading -= int64(tf.NumChunks())
//				chunksDownloaded += int64(tf.NumChunks())
//			}
//		case datas.DownloadFailed:
//			// Ignore for now and output errors on the main thread
//		}
//
//		str := fmt.Sprintf("%s of %s chunks complete. %s chunks being downloaded currently.", strhelp.CommaIfy(chunksDownloaded), strhelp.CommaIfy(chunks), strhelp.CommaIfy(chunksDownloading))
//		cliPos = cli.DeleteAndPrint(cliPos, str)
//	}
//
//	cli.Println()
//}
//
//func cloneRemote(ctx context.Context, srcDB *doltdb.DoltDB, remoteName, branch string, dEnv *env.DoltEnv) errhand.VerboseError {
//	eventCh := make(chan datas.TableFileEvent, 128)
//
//	wg := &sync.WaitGroup{}
//	wg.Add(1)
//	go func() {
//		defer wg.Done()
//		cloneProg(eventCh)
//	}()
//
//	err := actions.Clone(ctx, srcDB, dEnv.DoltDB, eventCh)
//	close(eventCh)
//
//	wg.Wait()
//
//	if err != nil {
//		if err == datas.ErrNoData {
//			err = errors.New("remote at that url contains no Dolt data")
//		}
//
//		return errhand.BuildDError("error: clone failed").AddCause(err).Build()
//	}
//
//	branches, err := dEnv.DoltDB.GetBranches(ctx)
//	if err != nil {
//		return errhand.BuildDError("error: failed to list branches").AddCause(err).Build()
//	}
//
//	if branch == "" {
//		for _, brnch := range branches {
//			branch = brnch.GetPath()
//			if branch == doltdb.MasterBranch {
//				break
//			}
//		}
//	}
//
//	// If we couldn't find a branch but the repo cloned successfully, it's empty. Initialize it instead of pulling from
//	// the remote.
//	performPull := true
//	if branch == "" {
//		err = initEmptyClonedRepo(ctx, dEnv)
//		if err != nil {
//			return nil
//		}
//
//		branch = doltdb.MasterBranch
//		performPull = false
//	}
//
//	cs, _ := doltdb.NewCommitSpec(branch)
//	cm, err := dEnv.DoltDB.Resolve(ctx, cs, nil)
//
//	if err != nil {
//		return errhand.BuildDError("error: could not get " + branch).AddCause(err).Build()
//	}
//
//	rootVal, err := cm.GetRootValue()
//	if err != nil {
//		return errhand.BuildDError("error: could not get the root value of " + branch).AddCause(err).Build()
//	}
//
//	// After actions.Clone, we have repository with a local branch for
//	// every branch in the remote. What we want is a remote branch ref for
//	// every branch in the remote. We iterate through local branches and
//	// create remote refs corresponding to each of them. We delete all of
//	// the local branches except for the one corresponding to |branch|.
//	for _, brnch := range branches {
//		cs, _ := doltdb.NewCommitSpec(brnch.GetPath())
//		cm, err := dEnv.DoltDB.Resolve(ctx, cs, nil)
//		if err != nil {
//			return errhand.BuildDError("error: could not resolve branch ref at " + brnch.String()).AddCause(err).Build()
//		}
//
//		remoteRef := ref.NewRemoteRef(remoteName, brnch.GetPath())
//		err = dEnv.DoltDB.SetHeadToCommit(ctx, remoteRef, cm)
//		if err != nil {
//			return errhand.BuildDError("error: could not create remote ref at " + remoteRef.String()).AddCause(err).Build()
//		}
//
//		if brnch.GetPath() != branch {
//			err := dEnv.DoltDB.DeleteBranch(ctx, brnch)
//			if err != nil {
//				return errhand.BuildDError("error: could not delete local branch " + brnch.String() + " after clone.").AddCause(err).Build()
//			}
//		}
//	}
//
//	if performPull {
//		err = actions.SaveDocsFromRoot(ctx, rootVal, dEnv)
//		if err != nil {
//			return errhand.BuildDError("error: failed to update docs on the filesystem").AddCause(err).Build()
//		}
//	}
//
//	h, err := dEnv.DoltDB.WriteRootValue(ctx, rootVal)
//	if err != nil {
//		return errhand.BuildDError("error: could not write root value").AddCause(err).Build()
//	}
//
//	dEnv.RepoState.Head = ref.MarshalableRef{Ref: ref.NewBranchRef(branch)}
//	dEnv.RepoState.Staged = h.String()
//	dEnv.RepoState.Working = h.String()
//
//	err = dEnv.RepoState.Save(dEnv.FS)
//	if err != nil {
//		return errhand.BuildDError("error: failed to write repo state").AddCause(err).Build()
//	}
//
//	return nil
//}
//
//// Inits an empty, newly cloned repo. This would be unnecessary if we properly initialized the storage for a repository
//// when we created it on dolthub. If we do that, this code can be removed.
//func initEmptyClonedRepo(ctx context.Context, dEnv *env.DoltEnv) error {
//	name := dEnv.Config.GetStringOrDefault(env.UserNameKey, "")
//	email := dEnv.Config.GetStringOrDefault(env.UserEmailKey, "")
//
//	if *name == "" {
//		return errhand.BuildDError(fmt.Sprintf("error: could not determine user name. run dolt config --global --add %[1]s", env.UserNameKey)).Build()
//	} else if *email == "" {
//		return errhand.BuildDError("error: could not determine email. run dolt config --global --add %[1]s", env.UserEmailKey).Build()
//	}
//
//	err := dEnv.InitDBWithTime(ctx, types.Format_Default, *name, *email, doltdb.CommitNowFunc())
//	if err != nil {
//		return errhand.BuildDError("error: could not initialize repository").AddCause(err).Build()
//	}
//
//	return nil
//}
