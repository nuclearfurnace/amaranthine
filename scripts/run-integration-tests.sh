#!/bin/bash

# Gotta have Synchrotron available.
cargo build

pushd synchrotron-test
cargo test
RESULT=$?
popd
exit $RESULT
