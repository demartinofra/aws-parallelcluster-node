#!/usr/bin/env python
# Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the
# License. A copy of the License is located at
#
# http://aws.amazon.com/asl/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

__author__ = 'dougalb'

import json
import time
import os
import sys
import ConfigParser

import boto.sqs
import boto.ec2
import boto.dynamodb
import boto.dynamodb2
import boto.exception
import daemon
import daemon.pidfile
from boto.sqs.message import RawMessage
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.table import Table


def getConfig():
    print('running getConfig')

    config = ConfigParser.RawConfigParser()
    config.read('sqswatcher.cfg')
    _region = config.get('sqswatcher', 'region')
    _sqsqueue = config.get('sqswatcher', 'sqsqueue')
    _table_name = config.get('sqswatcher', 'table_name')
    _scheduler = config.get('sqswatcher', 'scheduler')

    return _region, _sqsqueue, _table_name, _scheduler


def setupQueue(region, sqsqueue):
    print('running setupQueue')

    conn = boto.sqs.connect_to_region(region)

    _q = conn.get_queue(sqsqueue)
    if _q != None:
        _q.set_message_class(RawMessage)
    return _q


def setupDDBTable(region, table_name):
    print('running setupDDBTable')

    conn = boto.dynamodb.connect_to_region(region)
    tables = conn.list_tables()
    check = [t for t in tables if t == table_name]
    conn = boto.dynamodb2.connect_to_region(region)
    if check:
        _table = Table(table_name,connection=conn)
    else:
        _table = Table.create(table_name,
                              schema=[HashKey('instanceId')
                              ],connection=conn)

    return _table


def loadSchedulerModule(scheduler):
    print 'running loadSchedulerModule'

    scheduler = 'plugins.' + scheduler
    _scheduler = __import__(scheduler)
    _scheduler = sys.modules[scheduler]

    return _scheduler


def pollQueue():
    print 'running pollQueue'
    s = loadSchedulerModule(scheduler)

    while True:

        results = q.get_messages(10)

        while len(results) > 0:

            for result in results:
                message = json.loads(result.get_body())
                message_attrs = json.loads(message['Message'])
                eventType = message_attrs['Event']

                if eventType == 'autoscaling:TEST_NOTIFICATION':
                    print eventType
                    q.delete_message(result)

                if eventType != 'autoscaling:TEST_NOTIFICATION':
                    instanceId = message_attrs['EC2InstanceId']
                    if eventType == 'cfncluster:COMPUTE_READY':
                        print eventType, instanceId

                        ec2 = boto.connect_ec2()
                        ec2 = boto.ec2.connect_to_region(region)

                        retry = 0
                        wait = 15
                        while retry < 3:
                            try:
                                hostname = ec2.get_all_instances(instance_ids=instanceId)[
                                           0].instances[0].private_dns_name.split('.')[:1][0]
                                break
                            except boto.exception.BotoServerError as e:
                                if e.error_code == 'RequestLimitExceeded':
                                    time.sleep(wait)
                                    retry += 1
                                    wait = (wait*2+retry)
                                else:
                                    raise e

                        s.addHost(hostname)

                        t.put_item(data={
                            'instanceId': instanceId,
                            'hostname': hostname
                        })

                        q.delete_message(result)

                    elif eventType == 'autoscaling:EC2_INSTANCE_TERMINATE':
                        print eventType, instanceId

                        try:
                            item = t.get_item(consistent=True, instanceId=instanceId)
                            hostname = item['hostname']

                            s.removeHost(hostname)

                            item.delete()

                        except TypeError:
                            print ("Did not find %s in the metadb\n" % instanceId)

                        q.delete_message(result)

            results = q.get_messages(10)

        time.sleep(30)


def run():
    print('running run')
    with daemon.DaemonContext(detach_process=True, stderr=logfile, stdout=logfile,
                              working_directory=os.getcwd(),
                              pidfile=daemon.pidfile.TimeoutPIDLockFile('sqswatcher.pid')):
        print('about to call pollQueue')
        pollQueue()


if __name__ == "__main__":
    print('running __main__')
    logfile = open('sqswatcher.log', 'w')
    print >> logfile, time.ctime()
    logfile.flush()
    region, sqsqueue, table_name, scheduler = getConfig()
    q = setupQueue(region, sqsqueue)
    t = setupDDBTable(region, table_name)
    #s = loadSchedulerModule(scheduler)
    run()
