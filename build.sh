#!/bin/bash

mvn clean install -DskipTests

docs/_bin/generate-license-dependency-reports.py . distribution/target --clean-maven-artifact-transfer

MAVEN_OPTS='-Xmx3000m' mvn -DskipTests -Dforbiddenapis.skip=true -Dcheckstyle.skip=true -Dpmd.skip=true -Dmaven.javadoc.skip=true -pl '!benchmarks' -B --fail-at-end install -Pdist -Pbundle-contrib-exts
