#!/bin/bash
. "$(dirname $0)/variables.sh"

set -e

TARGET=${1:-profile.cov}
LOG=${2:-test.log}

rm $TARGET &>/dev/null || true
echo "mode: count" > $TARGET
echo "" > $LOG

DIRS=""
for DIR in $SRC;
do
  if ls $DIR/*_test.go &> /dev/null; then
    DIRS="$DIRS $DIR"
  fi
done

if [ "$NPROC" = "" ]; then
  NPROC=$(getconf _NPROCESSORS_ONLN)
fi

echo "test-cover begin: concurrency $NPROC"

PROFILE_REG="profile_reg.tmp"
PROFILE_BIG="profile_big.tmp"

TEST_FLAGS="-v -race -timeout 5m -covermode atomic"
go run .ci/gotestcover/gotestcover.go $TEST_FLAGS -coverprofile $PROFILE_REG -parallelpackages $NPROC $DIRS | tee $LOG
TEST_EXIT=${PIPESTATUS[0]}

# run big tests one by one
echo "test-cover begin: concurrency 1, +big"
for DIR in $DIRS; do
  if cat $DIR/*_test.go | grep "// +build" | grep "big" &>/dev/null; then
    # extract only the tests marked "big"
    BIG_TESTS=$(cat <(go test $DIR -tags big -list '.*' | grep -v '^ok' | grep -v 'no test files' ) \
                    <(go test $DIR -list '.*' | grep -v '^ok' | grep -v 'no test files')            \
                    | sort | uniq -u | paste -sd'|' -)
    go test $TEST_FLAGS -tags big -run $BIG_TESTS -coverprofile $PROFILE_BIG $DIR | tee $LOG
    BIG_TEST_EXIT=${PIPESTATUS[0]}
    # Only set TEST_EXIT if its already zero to be prevent overwriting non-zero exit codes
    if [ "$TEST_EXIT" = "0" ]; then
      TEST_EXIT=$BIG_TEST_EXIT
    fi
    if [ "$BIG_TEST_EXIT" != "0" ]; then
      continue
    fi
    if [ -s $PROFILE_BIG ]; then
      cat $PROFILE_BIG | tail -n +2 >> $PROFILE_REG
    fi
  fi
done

cat $PROFILE_REG | grep -v "_mock.go" > $TARGET

find . -not -path '*/vendor/*' | grep \\.tmp$ | xargs -I{} rm {}
echo "test-cover result: $TEST_EXIT"

exit $TEST_EXIT
