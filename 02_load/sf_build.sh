#!/usr/bin/env bash

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2017-2018 MonetDB Solutions B.V.

# Arguments:
# $1 - scale factor as "SF-N"
# $2 - port number
# $3 - worker_id"

TIMEFORMAT="%R"
if [ -z "$2" ]; then
  port=50000
else
  port="$2"
fi
monetdb -p "$port" stop "$1"
monetdb -p "$port" destroy "$1" -f
monetdb -p "$port" create "$1"
monetdb -p "$port" release "$1"
date
time mclient -d "$1" -ei tpch_schema.sql
date

# Generate the counts file if it does not exist or if its size is zero
if [ ! -s $1.counts ]; then
  wc -l $PWD/$1/data/$3/* | grep -v total | sort -n > $1.counts
fi

# Generate the copy into file
awk -f copy_into.awk $1.counts > $1.load

# Generate the verification file
awk -f verify.awk $1.counts > $1.verify

echo "Loading data"
time mclient -d "$1" -p "$port" -ei $1.load
date

# TODO: This breaks in a cluster context
#   Not all partitions of a table may have all their primary keys... thus adding a foreign key constraint might break
#   since it can't find the primary key in the partition of the table that is right next to it
#echo "Adding constraints (only primary key)"
#time mclient -d "$1" -p "$port" -ei tpch_alter.sql
#date

echo "Verifying: all the numbers should be zero"
verify_file=/tmp/verify_load_new  # TODO: make this a tempfile
mclient -d "$1" -p "$port" -f csv $1.verify | tee "$verify_file"

cmp ground_truth "$verify_file"
if [ $? == 0 ]; then
    rm "$verify_file"
else
    echo "Something went wrong. Review and then delete ${verify_file}"
    exit 1
fi

echo "Renaming tables"
awk -f rename.awk $1.counts > $1.rename
sed -i "s/_NUMBER/_$3/" $1.rename

time mclient -d "$1" -p "$port" -ei $1.rename
rm -rf $1.rename
date
