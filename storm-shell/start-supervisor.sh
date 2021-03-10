#!/bin/bash

nohup /home/opt/modules/apache-storm-1.2.3/bin/storm supervisor &
nohup /home/opt/modules/apache-storm-1.2.3/bin/storm logviewer &



