#!/bin/bash

. /etc/profile
bin=/opt/modules/apache-storm-1.2.1/bin/storm-shell/
has=/opt/modules/apache-storm-1.2.1/bin/storm-shell/storm-hosts-h
supervisors=/opt/modules/apache-storm-1.2.1/bin/storm-shell/storm-hosts

nohup storm ui &

#start nimbus
cat $has | while read ha
do
 echo $bin

 ssh $ha $bin/start-nimbus.sh &
done
   
#start supervisor & logview  
cat $supervisors | while read supervisor
do
echo $supervisor

   ssh $supervisor $bin/start-supervisor.sh &
done


