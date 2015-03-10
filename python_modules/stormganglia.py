#!/usr/bin/env python


"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

AUTHOR:Juan Carlos Fernandez
"""
# vim: ts=4:sw=4:et:sts=4:ai:tw=80

import stormStatus
import threading, time

class StormArgs:
    def __init__(self,serv,port,topology):
    	self.nimbus_serv=serv
    	self.nimbus_port=port
    	self.topology=topology

def feeder(name):
    topology,component,bolt1,bolt2,metric=name.rsplit('-',4)[0:5]
    bolt=bolt1+'-'+bolt2
    while storm_lock:
        time.sleep(1)
    for aux in storm[topology]['components'][component]['bolts']:
        if aux['id']==bolt:
            return aux['stats'][metric]['600']

def update_storm():
    global storm,storm_lock
    storm_lock = 1
    storm = stormStatus.StormStatus(StormArgs(nimbus,port,topology)).topologies
    storm_lock = 0
    threading.Timer(60.0,update_storm).start()

def metric_init(params):
    global storm,nimbus,port,topology
    descriptors=[]
    
    nimbus = params.get('nimbus_serv', 'localhost')
    port = params.get('nimbus_port', 6627)
    topology = params.get('topology',None)
    metrics_white_list = params.get('metrics_white_list',['process_ms_avg','executed','execute_ms_avg','acked','failed','load'])
    update_storm()
    while storm_lock:
        time.sleep(1)
    for topology_key,topology_val in storm.iteritems():
        for component_key,component_val in topology_val['components'].iteritems():
            for bolt in component_val['bolts']:
                name = topology_key+"-"+str(component_key)+"-"+str(bolt['id'])
                for metric in metrics_white_list:
                    print metric
                    d = {'name': name+"-"+metric,
                    'call_back': feeder,
                    'time_max': 90,
                    'value_type': 'uint',
                    'units': 'msg',
                    'slope': 'both',
                    'format': '%d',
                    'description': 'Bolt %s 10m' % metric,
                    'groups': 'Storm'+topology_key}
                    descriptors.append(d)
    return descriptors

def metric_cleanup():
    '''Clean up the metric module.'''
    pass

import time
#This code is for debugging and unit testing
if __name__ == '__main__':
    for x in range(5):
        descriptors=metric_init({})
        for d in descriptors:
            v = d['call_back'](d['name'])
            print 'value for %s' % d['name']
            print v
        time.sleep(2)

