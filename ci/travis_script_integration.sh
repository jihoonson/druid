#!/usr/bin/env bash

set -e

mvn clean install -DskipTests

pushd $TRAVIS_BUILD_DIR/integration-tests

mvn verify -P integration-tests

popd
