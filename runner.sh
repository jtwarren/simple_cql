#!/bin/bash

rps=(12000 13000 14000 15000 16000 17000 18000 19000 20000)

for r in "${rps[@]}"
do

echo `cd scripts; python error_data.py $r | grep "qwerty" 2>&1 | tee -a ../wheee.txt; cd -; ant runcqltest -Dtest=PerformanceErrorTest | grep "qwerty" 2>&1 | tee -a wheee.txt`

done
