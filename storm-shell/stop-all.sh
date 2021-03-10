#!/bin/bash

. /etc/profile
bin=/home/opt/modules/apache-storm-1.2.3/bin/storm-shell
has=/home/opt/modules/apache-storm-1.2.3/bin/storm-shell/storm-hosts-h
supervisors=/home/opt/modules/apache-storm-1.2.3/bin/storm-shell/storm-hosts


kill -9 `ps -ef|grep storm.ui.core |awk '{print $2}'`

#stop nimbus
cat $has | while read ha
do
echo $ha
   ssh $ha $bin/stop-nimbus.sh &
done
   
#stop supervisor & logviewer
cat $supervisors | while read supervisor
do
echo $supervisor
   ssh $supervisor $bin/stop-supervisor.sh &
done

