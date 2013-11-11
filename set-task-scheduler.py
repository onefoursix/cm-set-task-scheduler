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
##        (CM connection properties are set in the script below)
## 
## *******************************************************************************************

## ** imports *******************************

import sys
from cm_api.api_client import ApiResource


## ** Settings ******************************

## Cloudera Manager Host
cm_host = "localhost"
cm_port = "7180"

## Cloudera Manager login
cm_login = "admin"

## Cloudera Manager password
cm_password = "admin"

## Cluster Name
cluster_name = "Cluster 1 - CDH4"

## Service config property name
config_property_name = "mapred_capacity_scheduler_configuration"   # Capacity Scheduler config property name
# config_property_name = "mapred_fairscheduler_allocation"         # Fair Scheduler config property name

## ******************************************

if len(sys.argv) != 2:
  print "Error: Wrong number of arguments"
  print "Usage: set-task-scheduler.py  <config-file>"
  print "Example: set-task-scheduler.py conf/capacity-scheduler-1.xml"
  quit(1)


config_file = sys.argv[1]

print "\nSetting task scheduler config file: " + config_file

## load the task scheduler config file
f = open(config_file,'r')
task_scheduler_conf = ""
while 1:
    line = f.readline()
    if not line:break
    task_scheduler_conf += line
f.close()

## Get the CM api
api = ApiResource(server_host=cm_host, server_port=cm_port, username=cm_login, password=cm_password)

## Get the cluster
cluster = api.get_cluster(cluster_name)
print "Cluster: " + cluster_name


## Get the MR Service
for service in cluster.get_all_services():
  if service.type == "MAPREDUCE":
    mr_service = service

print "MapReduce Service :" + mr_service.name 


## Get the JobTracker base config group
for role_config_group in mr_service.get_all_role_config_groups():
  if role_config_group.name == mr_service.name + "-JOBTRACKER-BASE":
    job_tracker_base = role_config_group

## Set the task scheduler in the base config of the MR Service
job_tracker_base.update_config({config_property_name : task_scheduler_conf})

print "\nNew task scheduler configuration set\n"

## Refresh the JobTracker(s)
for role in mr_service.get_all_roles():
  if role.type == "JOBTRACKER":
    print "Refreshing the Job Tracker on " + role.hostRef.hostId
    mr_service.refresh(role.name)

print "\nDone\n\n"
