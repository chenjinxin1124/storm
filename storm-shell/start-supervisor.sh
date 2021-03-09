#!/bin/bash

. /etc/profile
nohup storm supervisor &
nohup storm logviewer &



