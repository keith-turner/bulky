#!/bin/bash

hadoop fs -rm -R /bulk0*
accumulo shell -u root -p secret -e 'deletetable -f bulky'
mvn package
./bin/run.sh cmd.Generate IA 100 1000 1000000 /bulk01
./bin/run.sh cmd.Generate IB 2 1000 2000 /bulk02
./bin/run.sh cmd.Split 100
./bin/run.sh cmd.Import /bulk01
./bin/run.sh cmd.Import /bulk02
./bin/run.sh cmd.Verify
