#!/usr/bin/env bash

mvn clean
echo "done clean"
mvn compile
echo "done compile"
mvn package -DskipTests
echo "done package"

