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

package nbs

import (
	"context"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestFileAppendixLoadIfExists(t *testing.T) {
	assert := assert.New(t)
	fm := makeFileManifestTempDir(t)
	defer os.RemoveAll(fm.dir)

	stats := &Stats{}

	exists, upstream, err := fm.ParseAppendixIfExists(context.Background(), stats, nil)
	require.NoError(t, err)
	assert.False(exists)

	// Simulate another process writing an appendix (with an old Noms version).
	jerk := computeAddr([]byte("jerk"))
	newRoot := hash.Of([]byte("new root"))
	tableName := hash.Of([]byte("table1"))
	gcGen := addr{}
	m := strings.Join([]string{StorageVersion, "0", jerk.String(), newRoot.String(), gcGen.String(), tableName.String(), "0"}, ":")
	err = clobberAppendix(fm.dir, m)
	require.NoError(t, err)

	// ParseAppendixIfExists should now reflect the appendix written above.
	exists, upstream, err = fm.ParseAppendixIfExists(context.Background(), stats, nil)
	require.NoError(t, err)
	assert.True(exists)
	assert.Equal("0", upstream.vers)
	assert.Equal(jerk, upstream.lock)
	assert.Equal(newRoot, upstream.root)
	if assert.Len(upstream.specs, 1) {
		assert.Equal(tableName.String(), upstream.specs[0].name.String())
		assert.Equal(uint32(0), upstream.specs[0].chunkCount)
	}
}

func TestFileAppendixLoadIfExistsHoldsLock(t *testing.T) {
	assert := assert.New(t)
	fm := makeFileManifestTempDir(t)
	defer os.RemoveAll(fm.dir)
	stats := &Stats{}

	// Simulate another process writing a appendix.
	lock := computeAddr([]byte("locker"))
	newRoot := hash.Of([]byte("new root"))
	tableName := hash.Of([]byte("table1"))
	gcGen := addr{}
	m := strings.Join([]string{StorageVersion, constants.NomsVersion, lock.String(), newRoot.String(), gcGen.String(), tableName.String(), "0"}, ":")
	err := clobberAppendix(fm.dir, m)
	require.NoError(t, err)

	// ParseAppendixIfExists should now reflect the appendix written above.
	exists, upstream, err := fm.ParseAppendixIfExists(context.Background(), stats, func() error {
		// This should fail to get the lock, and therefore _not_ clobber the appendix.
		lock := computeAddr([]byte("newlock"))
		badRoot := hash.Of([]byte("bad root"))
		m = strings.Join([]string{StorageVersion, "0", lock.String(), badRoot.String(), gcGen.String(), tableName.String(), "0"}, ":")
		b, err := tryClobberAppendix(fm.dir, m)
		require.NoError(t, err, string(b))
		return err
	})

	require.NoError(t, err)
	assert.True(exists)
	assert.Equal(constants.NomsVersion, upstream.vers)
	assert.Equal(newRoot, upstream.root)
	if assert.Len(upstream.specs, 1) {
		assert.Equal(tableName.String(), upstream.specs[0].name.String())
		assert.Equal(uint32(0), upstream.specs[0].chunkCount)
	}
}

func TestFileAppendixUpdateEmpty(t *testing.T) {
	assert := assert.New(t)
	fm := makeFileManifestTempDir(t)
	defer os.RemoveAll(fm.dir)
	stats := &Stats{}

	l := computeAddr([]byte{0x01})
	upstream, err := fm.UpdateAppendix(context.Background(), addr{}, manifestContents{vers: constants.NomsVersion, lock: l}, stats, nil)
	require.NoError(t, err)
	assert.Equal(l, upstream.lock)
	assert.True(upstream.root.IsEmpty())
	assert.Empty(upstream.specs)

	fm2 := fileManifestV5{fm.dir} // Open existent, but empty appendix
	exists, upstream, err := fm2.ParseAppendixIfExists(context.Background(), stats, nil)
	require.NoError(t, err)
	assert.True(exists)
	assert.Equal(l, upstream.lock)
	assert.True(upstream.root.IsEmpty())
	assert.Empty(upstream.specs)

	l2 := computeAddr([]byte{0x02})
	upstream, err = fm2.UpdateAppendix(context.Background(), l, manifestContents{vers: constants.NomsVersion, lock: l2}, stats, nil)
	require.NoError(t, err)
	assert.Equal(l2, upstream.lock)
	assert.True(upstream.root.IsEmpty())
	assert.Empty(upstream.specs)
}

func TestFileAppendixUpdate(t *testing.T) {
	assert := assert.New(t)
	fm := makeFileManifestTempDir(t)
	defer os.RemoveAll(fm.dir)
	stats := &Stats{}

	// First, test winning the race against another process.
	contents := manifestContents{
		vers:  constants.NomsVersion,
		lock:  computeAddr([]byte("locker")),
		root:  hash.Of([]byte("new root")),
		specs: []tableSpec{{computeAddr([]byte("a")), 3}},
	}
	upstream, err := fm.UpdateAppendix(context.Background(), addr{}, contents, stats, func() error {
		// This should fail to get the lock, and therefore _not_ clobber the appendix. So the Update should succeed.
		lock := computeAddr([]byte("nolock"))
		newRoot2 := hash.Of([]byte("noroot"))
		gcGen := addr{}
		m := strings.Join([]string{StorageVersion, constants.NomsVersion, lock.String(), newRoot2.String(), gcGen.String()}, ":")
		b, err := tryClobberAppendix(fm.dir, m)
		require.NoError(t, err, string(b))
		return nil
	})
	require.NoError(t, err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)

	// Now, test the case where the optimistic lock fails, and someone else updated the root since last we checked.
	contents2 := manifestContents{lock: computeAddr([]byte("locker 2")), root: hash.Of([]byte("new root 2")), vers: constants.NomsVersion}
	upstream, err = fm.UpdateAppendix(context.Background(), addr{}, contents2, stats, nil)
	require.NoError(t, err)
	assert.Equal(contents.lock, upstream.lock)
	assert.Equal(contents.root, upstream.root)
	assert.Equal(contents.specs, upstream.specs)
	upstream, err = fm.UpdateAppendix(context.Background(), upstream.lock, contents2, stats, nil)
	require.NoError(t, err)
	assert.Equal(contents2.lock, upstream.lock)
	assert.Equal(contents2.root, upstream.root)
	assert.Empty(upstream.specs)

	// Now, test the case where the optimistic lock fails because someone else updated only the tables since last we checked
	jerkLock := computeAddr([]byte("jerk"))
	tableName := computeAddr([]byte("table1"))
	gcGen := addr{}
	m := strings.Join([]string{StorageVersion, constants.NomsVersion, jerkLock.String(), contents2.root.String(), gcGen.String(), tableName.String(), "1"}, ":")
	err = clobberAppendix(fm.dir, m)
	require.NoError(t, err)

	contents3 := manifestContents{lock: computeAddr([]byte("locker 3")), root: hash.Of([]byte("new root 3")), vers: constants.NomsVersion}
	upstream, err = fm.UpdateAppendix(context.Background(), upstream.lock, contents3, stats, nil)
	require.NoError(t, err)
	assert.Equal(jerkLock, upstream.lock)
	assert.Equal(contents2.root, upstream.root)
	assert.Equal([]tableSpec{{tableName, 1}}, upstream.specs)
}

// tryClobberAppendix simulates another process trying to access dir/appendixFileName concurrently. To avoid deadlock, it does a non-blocking lock of dir/lockFileName. If it can get the lock, it clobbers the appendix.
func tryClobberAppendix(dir, contents string) ([]byte, error) {
	return runClobberAppendix(dir, contents)
}

// clobberAppendix simulates another process writing dir/appendixFileName concurrently. It ignores the lock file, so it's up to the caller to ensure correctness.
func clobberAppendix(dir, contents string) error {
	if err := ioutil.WriteFile(filepath.Join(dir, lockFileName), nil, 0666); err != nil {
		return err
	}
	return ioutil.WriteFile(filepath.Join(dir, appendixFileName), []byte(contents), 0666)
}

func runClobberAppendix(dir, contents string) ([]byte, error) {
	_, filename, _, _ := runtime.Caller(1)
	clobber := filepath.Join(filepath.Dir(filename), "test/manifest_clobber.go")
	mkPath := func(f string) string {
		return filepath.Join(dir, f)
	}
	c := exec.Command("go", "run", clobber, mkPath(lockFileName), mkPath(appendixFileName), contents)
	return c.CombinedOutput()
}
