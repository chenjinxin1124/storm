#!/bin/bash

. /etc/profile
bin=/opt/modules/apache-storm-1.2.1/bin/storm-shell/
has=/opt/modules/apache-storm-1.2.1/bin/storm-shell/storm-hosts-h
supervisors=/opt/modules/apache-storm-1.2.1/bin/storm-shell/storm-hosts


kill -9 `ps -ef|grep storm.ui.core |awk '{print $2}'`


#stop nimbus
cat $has | while read ha
do
echo $ha

   ssh $ha $bin/stop-nimbus.sh &
done
   
#stop supervisor & logview  
cat $supervisors | while read supervisor
do
echo $supervisor

   ssh $supervisor $bin/stop-supervisor.sh &
done

