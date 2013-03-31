#!/bin/sh

LN=1
echo "ITERS: $1"
while [[ $LN -le $1 ]]; do
    sleep 0.1
    echo "PID: $$, LINE: $LN" 
    LN=$((LN+1))
done
echo 'TERMINATED'
