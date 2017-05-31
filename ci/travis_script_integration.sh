#!/usr/bin/env bash

pushd $TRAVIS_BUILD_DIR/integration-tests

mvn verify -P integration-tests

popd
