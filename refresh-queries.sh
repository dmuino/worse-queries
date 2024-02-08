#!/bin/sh
#

echo Creating queries.txt
nflxlog --format=csv q --app firewoodqueryapi --env test "formattedMessage,storageStack=GLOBAL,:contains,nf.env,test,:eq,:and" \
  --fields formattedMessage --start e-6h --end now --limit=50000 | \
  perl -nle 's/"executing query=//;s/, database=clickhouse.*//; /SELECT/ && print' > queries.txt

echo Found $(wc -l queries.txt) queries
