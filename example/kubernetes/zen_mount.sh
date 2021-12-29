#!/bin/bash
if [[ $# -ne 2 ]];then
  echo "zen_mount.sh namespace true/false"
fi

if [[ "$2" == "true" ]];then
  oc patch -n $1 deploy usermgmt ibm-nginx zen-core-api -p '{"spec":{"replicas":2}}'
  oc patch -n $1 deploy zen-data-sorcerer zen-watchdog zen-watcher -p '{"spec":{"replicas":1}}'
elif [[ "$2" == "false" ]];then
  oc patch -n $1 deploy usermgmt ibm-nginx zen-core-api -p '{"spec":{"replicas":0}}'
  oc patch -n $1 deploy zen-data-sorcerer zen-watchdog zen-watcher -p '{"spec":{"replicas":0}}'
fi


