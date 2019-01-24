#!/bin/sh

mkdir log/

a=0
while [ $a -lt 5 ]
do
   python fake-log.py -n 1000 -o LOG -p log/
   a=`expr $a + 1`
done

hdfs dfs -put log /input

