#!/bin/bash

. /etc/profile
kill -9 `ps -ef|grep storm.daemon.supervisor.Supervisor |awk '{print $2}'`
kill -9 `ps -ef|grep storm.daemon.logviewer |awk '{print $2}'`



