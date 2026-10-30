#!/bin/sh
set -eu
for topic in HmdpBusiness HmdpOrderDeadline; do
  sh mqadmin updateTopic -n mq-nameserver:9876 -b rocketmq:10911 -t "$topic" -r 4 -w 4
done
