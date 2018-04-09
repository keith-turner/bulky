#!/bin/bash

BULK_API=new

hadoop fs -rm -R /bulk0*
accumulo shell -u root -p secret -e 'deletetable -f bulky'
mvn package
./bin/run.sh cmd.Generate IA 100 10000 1000000 /bulk01
./bin/run.sh cmd.Generate IB 100 1 1000 /bulk02
./bin/run.sh cmd.Generate IC 10 1 1000 /bulk03
./bin/run.sh cmd.Generate ID 10 10 10000 /bulk04
./bin/run.sh cmd.Split 100
./bin/run.sh cmd.Import $BULK_API /bulk01
./bin/run.sh cmd.Import $BULK_API /bulk02
./bin/run.sh cmd.Import $BULK_API /bulk03
./bin/run.sh cmd.Import $BULK_API /bulk04
./bin/run.sh cmd.Verify
