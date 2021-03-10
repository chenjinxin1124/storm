#!/bin/bash

bin=/home/opt/modules/apache-storm-1.2.3/bin/storm-shell
has=/home/opt/modules/apache-storm-1.2.3/bin/storm-shell/storm-hosts-h
supervisors=/home/opt/modules/apache-storm-1.2.3/bin/storm-shell/storm-hosts

nohup /home/opt/modules/apache-storm-1.2.3/bin/storm ui &

#start nimbus
cat $has | while read ha
do
 echo $bin
 ssh $ha $bin/start-nimbus.sh &
done
   
#start supervisor & logviewer
cat $supervisors | while read supervisor
do
echo $supervisor
   ssh $supervisor $bin/start-supervisor.sh &
done


