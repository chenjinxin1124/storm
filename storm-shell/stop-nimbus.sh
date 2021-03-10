#!/bin/bash
. /etc/profile
kill -9 `ps -ef|grep storm.daemon.nimbus |awk '{print $2}'`


