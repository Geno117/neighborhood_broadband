#!/bin/bash 

sk_psql="psql -p5433 -d sk -U geno -f "

#all normalprocessing scripts for sanity
$sk_psql cleanup2019/1_general_clean1.sql
$sk_psql cleanup2019/3_remove_bad_servers.sql
# removing people who move ISPs and other exluded points (specific by year/month?)
$sk_psql cleanup2019/2_movers_and_exclusions.sql