# Copyright 2013-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the
# License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
import collections
import logging
import os
import socket
import subprocess
import time
from tempfile import NamedTemporaryFile

import paramiko

import common.sge as sge
from common.sge import check_sge_command_output, run_sge_command

log = logging.getLogger(__name__)

SGEHostTypeConfig = collections.namedtuple("SGEHostTypeConfig", ["command_flags", "successful_messages", "host_type"])


def _is_host_configured(command, hostname):
    output = check_sge_command_output(command)
    # Expected output
    # ip-172-31-66-16.ec2.internal
    # ip-172-31-74-69.ec2.internal
    match = list(filter(lambda x: hostname in x.split(".")[0], output.split("\n")))
    return True if len(match) > 0 else False


def _add_hosts_by_type(hostnames, host_type_config):
    try:
        log.info("Adding hosts %s as %s host", ",".join(hostnames), host_type_config.host_type)
        command = "qconf {flags} {hostnames}".format(flags=host_type_config.command_flags, hostnames=",".join(hostnames))
        output = check_sge_command_output(command)
        failed_hosts = succeeded_hosts = []
        log.info(output)
        for hostname in hostnames:
            successful_messages = [message.format(hostname=hostname) for message in host_type_config.successful_messages]
            if any(message in output for message in successful_messages):
                succeeded_hosts.append(hostname)
            else:
                failed_hosts.append(hostname)

        return succeeded_hosts, failed_hosts
    except Exception as e:
        log.error("Unable to add hosts %s as %s host. Failed with exception %s", ",".join(hostnames),
                  host_type_config.host_type, e)
        return [], hostnames


HOST_TYPE_TO_CONFIG_MAP = {
    "ADMINISTRATIVE": SGEHostTypeConfig(
        command_flags="-ah",
        successful_messages=['"{hostname}" added to administrative host list', 'adminhost "{hostname}" already exists'],
        host_type="administrative",
    ),
    "SUBMIT": SGEHostTypeConfig(
        command_flags="-as",
        successful_messages=['"{hostname}" added to submit host list', 'submithost "{hostname}" already exists'],
        host_type="submit",
    )
}


def _add_hosts(hosts):
    hostnames = [host.hostname for host in hosts]
    for host_type in ["ADMINISTRATIVE", "SUBMIT"]:
        succeeded_hosts, failed_hosts = _add_hosts_by_type(hostnames, HOST_TYPE_TO_CONFIG_MAP[host_type])
        if failed_hosts:
            return succeeded_hosts, failed_hosts

    failed_hosts = succeeded_hosts = []
    for host in hosts:
        # Add the host to the all.q
        try:
            command = "qconf -aattr hostgroup hostlist %s @allhosts" % host.hostname
            run_sge_command(command)
        except Exception as e:
            log.warning("Unable to add host %s to all.q. Failed with exception %s", host.hostname, e)
            failed_hosts.append(host.hostname)
            continue

        # Set the numbers of slots for the host
        try:
            command = 'qconf -aattr queue slots ["%s=%s"] all.q' % (host.hostname, host.slots)
            run_sge_command(command)
        except Exception as e:
            log.warning("Unable to set the number of slots for the host %s. Failed with exception %s", host.hostname, e)
            failed_hosts.append(host.hostname)
            continue

        succeeded_hosts.append(host.hostname)

    return failed_hosts, succeeded_hosts


def addHost(hostname, cluster_user, slots, max_cluster_size):
    log.info("Adding %s with %s slots" % (hostname, slots))

    # Adding host as administrative host
    try:
        command = "qconf -ah %s" % hostname
        run_sge_command(command)
    except subprocess.CalledProcessError:
        log.warning("Unable to add host %s as administrative host", hostname)

    # Adding host as submit host
    try:
        command = "qconf -as %s" % hostname
        run_sge_command(command)
    except subprocess.CalledProcessError:
        log.warning("Unable to add host %s as submission host", hostname)

    # Setup template to add execution host
    qconf_Ae_template = """hostname              %s
load_scaling          NONE
complex_values        NONE
user_lists            NONE
xuser_lists           NONE
projects              NONE
xprojects             NONE
usage_scaling         NONE
report_variables      NONE
"""

    with NamedTemporaryFile() as t:
        temp_template = open(t.name, "w")
        temp_template.write(qconf_Ae_template % hostname)
        temp_template.flush()
        os.fsync(t.fileno())

        # Add host as an execution host
        try:
            command = "qconf -Ae %s" % t.name
            run_sge_command(command)
        except subprocess.CalledProcessError:
            log.warning("Unable to add host %s as execution host", hostname)

    # Connect and start SGE
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    hosts_key_file = os.path.expanduser("~" + cluster_user) + "/.ssh/known_hosts"
    user_key_file = os.path.expanduser("~" + cluster_user) + "/.ssh/id_rsa"
    iter = 0
    connected = False
    while iter < 3 and connected is False:
        try:
            log.info("Connecting to host: %s iter: %d" % (hostname, iter))
            ssh.connect(hostname, username=cluster_user, key_filename=user_key_file)
            connected = True
        except socket.error as e:
            log.error("Socket error: %s" % e)
            time.sleep(10 + iter)
            iter = iter + 1
            if iter == 3:
                log.critical("Unable to provision host")
                return
    try:
        ssh.load_host_keys(hosts_key_file)
    except IOError:
        ssh._host_keys_filename = None
        pass
    ssh.save_host_keys(hosts_key_file)
    command = (
        "sudo sh -c 'cd {0} && {0}/inst_sge -noremote -x -auto /opt/parallelcluster/templates/sge/sge_inst.conf'"
    ).format(sge.SGE_ROOT)
    stdin, stdout, stderr = ssh.exec_command(command)
    while not stdout.channel.exit_status_ready():
        time.sleep(1)
    ssh.close()

    # Add the host to the all.q
    try:
        command = "qconf -aattr hostgroup hostlist %s @allhosts" % hostname
        run_sge_command(command)
    except subprocess.CalledProcessError:
        log.warning("Unable to add host %s to all.q", hostname)

    # Set the numbers of slots for the host
    try:
        command = 'qconf -aattr queue slots ["%s=%s"] all.q' % (hostname, slots)
        run_sge_command(command)
    except subprocess.CalledProcessError:
        log.warning("Unable to set the number of slots for the host %s", hostname)


def removeHost(hostname, cluster_user, max_cluster_size):
    log.info("Removing %s", hostname)

    # Check if host is administrative host
    command = "qconf -sh"
    if _is_host_configured(command, hostname):
        # Removing host as administrative host
        command = "qconf -dh %s" % hostname
        run_sge_command(command)
    else:
        log.info("Host %s is not administrative host", hostname)

    # Check if host is in all.q (qconf -sq all.q)
    # Purge hostname from all.q
    try:
        command = "qconf -purge queue '*' all.q@%s" % hostname
        run_sge_command(command)
    except subprocess.CalledProcessError:
        log.warning("Unable to remove host %s from all.q", hostname)

    # Check if host is in @allhosts group (qconf -shgrp_resolved @allhosts)
    # Remove host from @allhosts group
    try:
        command = "qconf -dattr hostgroup hostlist %s @allhosts" % hostname
        run_sge_command(command)
    except subprocess.CalledProcessError:
        log.warning("Unable to remove host %s from @allhosts group", hostname)

    # Check if host is execution host
    command = "qconf -sel"
    if _is_host_configured(command, hostname):
        # Removing host as execution host
        command = "qconf -de %s" % hostname
        run_sge_command(command)
    else:
        log.info("Host %s is not execution host", hostname)

    # Check if host is submission host
    command = "qconf -ss"
    if _is_host_configured(command, hostname):
        # Removing host as submission host
        command = "qconf -ds %s" % hostname
        run_sge_command(command)
    else:
        log.info("Host %s is not submission host", hostname)


def update_cluster(max_cluster_size, cluster_user, update_events, instance_properties):
    failed = []
    succeeded = []
    hosts_to_add = []
    for event in update_events:
        try:
            if event.action == "REMOVE":
                removeHost(event.host.hostname, cluster_user, max_cluster_size)
            elif event.action == "ADD":
                # addHost(event.host.hostname, cluster_user, event.host.slots, max_cluster_size)
                hosts_to_add.append(event.host)
                continue
            succeeded.append(event)
        except Exception as e:
            log.error(
                "Encountered error when processing %s event for host %s: %s", event.action, event.host.hostname, e
            )
            failed.append(event)

    if hosts_to_add:
        failed_hosts, succeeded_hosts = _add_hosts(hosts_to_add)
        for event in update_events:
            if event.action == "ADD":
                if event.host.hostname in failed_hosts:
                    failed.append(event)
                else:
                    succeeded.append(event)

    return failed, succeeded
