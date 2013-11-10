#!/usr/bin/python

## *******************************************************************************************
## set-task-scheduler.py 
## 
## Example of how to set a Task Scheduler Configuration using the Cloudera Manager API,
##   as well as how to refresh a running Job Tracker to deploy the change without a restart
## 
## Usage: set-task-scheduler.py  <config-file>
##
##        for example:  set-task-scheduler.py conf/capacity-scheduler-1.xml
## 
##        (the rest of the values are set in the script below)
## 
## *******************************************************************************************

## ** imports *******************************

import sys
from cm_api.api_client import ApiResource



## ** Settings ******************************

## Cloudera Manager Host
cm_host = "mbrooks0.onefoursix.com"
cm_port = "7180"

## Cloudera Manager login
cm_login = "admin"

## Cloudera Manager password
cm_password = "admin"

## Cluster Name
cluster_name = "Cluster 1 - CDH4"

## Name of MapReduce Service
mr_service_name = "mapreduce1"

## Service config property name
config_property_name = "mapred_capacity_scheduler_configuration"   # Capacity Scheduler config property name
# config_property_name = "mapred_fairscheduler_allocation"         # Fair Scheduler config property name

## ******************************************


if len(sys.argv) != 2:
  print "Error: Wrong number of arguments"
  print "Usage: set-task-scheduler.py  <config-file>"
  print "Example: set-task-scheduler.py conf/capacity-scheduler-1.xml"
  quit(1)


config_file =  sys.argv[1]

print "Setting task scheduler config file '" + config_file + "' for MapReduce Service '" + mr_service_name + "' on cluster '" + cluster_name + "'..."

## load the task scheduler config file
f = open(config_file,'r')
task_scheduler_conf = ""
while 1:
    line = f.readline()
    if not line:break
    task_scheduler_conf += line
f.close()

print "-- loading new config --------------------------"
print task_scheduler_conf
print "------------------------------------------------"



## Get the CM api
api = ApiResource(server_host=cm_host, server_port=cm_port, username=cm_login, password=cm_password)

## Get the cluster
cluster = api.get_cluster(cluster_name)

## Get the MR Service
mr_service = cluster.get_service(mr_service_name)

## Get the JobTracker
for role in mr_service.get_all_roles():
#  if role.type == 'JOBTRACKER':
   print "role:" + role.type + " " + role.name  
   job_tracker = role

## Set the task scheduler config in the MR Service
#job_tracker.update_config({config_property_name : task_scheduler_conf})
mr_service.update_config({config_property_name : task_scheduler_conf})

print "New task scheduler configuration set\n"
print "Refreshing the Job Tracker"
print "Done"