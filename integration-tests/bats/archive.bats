#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common

    dolt sql -q "create table tbl (i int auto_increment primary key, guid char(36))"
    dolt commit -A -m "create tbl"

    dolt sql -q "$(insert_statement)"
}

teardown() {
    if [ -n "$remotesrv_pid" ]; then
        kill "$remotesrv_pid"
        wait "$remotesrv_pid" || :
        remotesrv_pid=""
    fi

    assert_feature_version
    teardown_common
}

# Inserts 25 new rows and commits them.
insert_statement() {
  res="INSERT INTO tbl (guid) VALUES (UUID());"
  for ((i=1; i<=24; i++))
  do
    res="$res INSERT INTO tbl (guid) VALUES (UUID());"
  done
  res="$res call dolt_commit(\"-A\", \"--allow-empty\", \"-m\", \"Add 25 values\");"
  echo "$res"
}

# Updates 10 random rows and commits the changes.
update_statement() {
  res="SET @max_id = (SELECT MAX(i) FROM tbl);
SET @random_id = FLOOR(1 + RAND() * @max_id);
UPDATE tbl SET guid = UUID() WHERE i >= @random_id LIMIT 1;"
  for ((i=1; i<=9; i++))
  do
    res="$res
SET @max_id = (SELECT MAX(i) FROM tbl);
SET @random_id = FLOOR(1 + RAND() * @max_id);
UPDATE tbl SET guid = UUID() WHERE i >= @random_id LIMIT 1;"
  done
  res="$res call dolt_commit(\"-A\", \"--allow-empty\", \"-m\", \"Update 10 values\");"
  echo "$res"
}

# A series of 10 update-and-commit-then-insert-and-commit pairs, followed by a dolt_gc call
#
# This is useful because we need at least 25 retained chunks to create a commit.
mutations_and_gc_statement() {
  query=`update_statement`
  for ((j=1; j<=9; j++))
  do
    query="$query $(insert_statement)"
    query="$query $(update_statement)"
  done
  query="$query $(insert_statement)"
  query="$query call dolt_gc();"
  echo "$query"
}

@test "archive: too few chunks" {
  dolt sql -q "$(update_statement)"
  dolt gc

  run dolt archive
  [ "$status" -eq 1 ]
  [[ "$output" =~ "Not enough samples to build default dictionary" ]] || false
}

@test "archive: require gc first" {
  run dolt archive
  [ "$status" -eq 1 ]
  [[ "$output" =~ "Run 'dolt gc' first" ]] || false
}

@test "archive: single archive" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive

  files=$(find . -name "*darc" | wc -l | sed 's/[ \t]//g')
  [ "$files" -eq "1" ]

  # Ensure updates continue to work.
  dolt sql -q "$(update_statement)"
}

@test "archive: multiple archives" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt sql -q "$(mutations_and_gc_statement)"

  dolt archive

  files=$(find . -name "*darc" | wc -l | sed 's/[ \t]//g')
  [ "$files" -eq "3" ]

  # dolt log --stat will load every single chunk.
  commits=$(dolt log --stat --oneline | wc -l | sed 's/[ \t]//g')
  [ "$commits" -eq "186" ]
}

@test "archive: archive multiple times" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive

  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive

  files=$(find . -name "*darc" | wc -l | sed 's/[ \t]//g')
  [ "$files" -eq "2" ]
}

@test "archive: archive --revert (fast)" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive
  dolt archive --revert

  # dolt log --stat will load every single chunk. 66 manually verified.
  commits=$(dolt log --stat --oneline | wc -l | sed 's/[ \t]//g')
  [ "$commits" -eq "66" ]
}

@test "archive: archive --revert (rebuild)" {
  dolt sql -q "$(mutations_and_gc_statement)"
  dolt archive
  dolt gc                         # This will delete the unused table files.
  dolt archive --revert

  # dolt log --stat will load every single chunk. 66 manually verified.
  commits=$(dolt log --stat --oneline | wc -l | sed 's/[ \t]//g')
  [ "$commits" -eq "66" ]
}

@test "archive: can clone archived repository" {
    mkdir -p remote/.dolt
    mkdir cloned

    # Copy the archive test repo to remote directory
    cp -R $BATS_TEST_DIRNAME/archive-test-repo/* remote/.dolt
    cd remote

    port=$( definePORT )

    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ../cloned
    run dolt clone http://localhost:$port/test-org/test-repo repo1
    [ "$status" -eq 0 ]
    cd repo1

    # Verify we can read data
    run dolt sql -q 'select sum(i) from tbl;'
    [[ "$status" -eq 0 ]] || false
    [[ "$output" =~ "138075" ]] || false # i = 1 - 525, sum is 138075

    kill $remotesrv_pid
    wait $remotesrv_pid || :
    remotesrv_pid=""

    ## The above test is the setup for the next test - so we'll stick both in here.
    ## This tests cloning from a clone. Archive files are generally in oldgen, but not the case with a fresh clone.
    cd ../../
    mkdir clone2

    cd cloned/repo1 # start the server using the clone from above.
    port=$( definePORT )
    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ../../clone2
    run dolt clone http://localhost:$port/test-org/test-repo repo2
    [ "$status" -eq 0 ]
    cd repo2

    run dolt sql -q 'select sum(i) from tbl;'
    [[ "$status" -eq 0 ]] || false
    [[ "$output" =~ "138075" ]] || false # i = 1 - 525, sum is 138075
}

@test "archive: can clone respiratory with mixed types" {
    mkdir -p remote/.dolt
    mkdir cloned

    # Copy the archive test repo to remote directory
    cp -R $BATS_TEST_DIRNAME/archive-test-repo/* remote/.dolt
    cd remote

    # Insert data (commits automatically), but don't gc/archive yet. Want to make sure we can still clone it.
    dolt sql -q "$(insert_statement)"

    port=$( definePORT )

    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ../cloned
    run dolt clone http://localhost:$port/test-org/test-repo repo1
    [ "$status" -eq 0 ]
    cd repo1

    # verify new data is there.
    run dolt sql -q 'select sum(i) from tbl;'
    [[ "$status" -eq 0 ]] || false

    [[ "$output" =~ "151525" ]] || false # i = 1 - 550, sum is 151525
}

@test "archive: can fetch chunks from an archived repo" {
    mkdir -p remote/.dolt
    mkdir cloned

    # Copy the archive test repo to remote directory
    cp -R $BATS_TEST_DIRNAME/archive-test-repo/* remote/.dolt
    cd remote

    port=$( definePORT )

    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ../cloned
    dolt clone http://localhost:$port/test-org/test-repo repo1
    # Fetch when there are no changes.
    cd repo1
    dolt fetch

    ## update the remote repo directly. Need to run the archive command when the server is stopped.
    ## This will result in archived files on the remote, which we will need to read chunks from when we fetch.
    cd ../../remote
    kill $remotesrv_pid
    wait $remotesrv_pid || :
    remotesrv_pid=""
    dolt sql -q "$(mutations_and_gc_statement)"
    dolt archive

    remotesrv --http-port $port --grpc-port $port --repo-mode &
    remotesrv_pid=$!
    [[ "$remotesrv_pid" -gt 0 ]] || false

    cd ../cloned/repo1
    dolt fetch

    run dolt status
    [ "$status" -eq 0 ]

    [[ "$output" =~ "Your branch is behind 'origin/main' by 20 commits, and can be fast-forwarded" ]] || false

    # Verify the repo has integrity.
    dolt fsck
}

@test "archive: backup and restore" {
  # cp the repository from the test dir.
  mkdir -p original/.dolt
  cp -R $BATS_TEST_DIRNAME/archive-test-repo/* original/.dolt

  cd original
  dolt backup add bac1 file://../bac1
  dolt backup sync bac1

  cd ..

  dolt backup restore file://./bac1 restored
  cd restored
  # Verify we can read data
  run dolt sql -q 'select sum(i) from tbl;'
  [[ "$status" -eq 0 ]] || false
  [[ "$output" =~ "138075" ]] || false # i = 1 - 525, sum is 138075
}