#!/bin/bash 

sk_psql="psql -p5433 -d sk -U geno -f "

#./build_db.sh
$sk_psql sql/schema
$sk_psql sql/3monthscopy_direct.sql
#./cleanup.sh causes errors which im not going to handle atm
#./cleanup.sh

for i in $(ls processing2019)
do
    $sk_psql "processing2019/$i"
    echo "processed " $i
done