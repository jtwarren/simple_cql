#!/bin/bash

rps=(1000 2000 3000 4000 5000 6000 7000 8000 9000 10000)

for r in "${rps[@]}"
do

echo `cd scripts; python trending_data.py $r | grep "qwerty" 2>&1 | tee -a ../trending_test.txt; cd -; ant runcqltest -Dtest=PerformanceTrendingTest | grep "qwerty" 2>&1 | tee -a trending_test.txt`

done
