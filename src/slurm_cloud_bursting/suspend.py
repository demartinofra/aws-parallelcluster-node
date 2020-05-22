import collections
import itertools
import logging
import subprocess
import time

import argparse
import boto3
from botocore.exceptions import ClientError

SCONTROL = "/opt/slurm/bin/scontrol"
LOGFILE = "/home/slurm/slurm_suspend.log"
REGION = "us-east-2"

TOT_REQ_CNT = 1000

operations = {}
retry_list = []


SlurmNode = collections.namedtuple("SlurmNode", ["name", "nodeaddr", "state"])


def _run_command(command, log_error=True, fail_on_error=True, env=None):
    """Execute shell command."""
    logging.info("Executing command: %s", command)
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        encoding="utf-8",
        env=env,
        shell=True,
    )
    try:
        result.check_returncode()
    except subprocess.CalledProcessError:
        if log_error:
            logging.error(
                "Command {0} failed with error:\n{1}\nand output:\n{2}".format(command, result.stderr, result.stdout)
            )
        if fail_on_error:
            raise

    return result


def _grouper(n, iterable):
    it = iter(iterable)
    return iter(lambda: list(itertools.islice(it, n)), ())


def delete_instances(ec2_instance_ids):
    ec2_client = boto3.client("ec2", region_name=REGION)
    batch_size = 10
    for instances in _grouper(batch_size, ec2_instance_ids):
        logging.info("Terminating instances %s", instances)
        try:
            ec2_client.terminate_instances(InstanceIds=instances,)
        except ClientError as e:
            logging.error("Failed when terminating instances %s with error %d", instances, e)
            retry_list.extend(instances)


def _get_nodes_info(nodes):
    show_nodeaddr_command = (
        f'{SCONTROL} show nodes {nodes} | grep -oP "^NodeName=\K(\S+)| NodeAddr=\K(\S+)| State=\K(\S+)"'
    )
    nodeaddr_str = _run_command(show_nodeaddr_command).stdout
    nodes = []
    for node in _grouper(3, nodeaddr_str.splitlines()):
        nodes.append(SlurmNode(*node))
    return nodes


def _grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def _get_instance_ids(nodeaddr_list):
    ec2_client = boto3.client("ec2", region_name=REGION)
    paginator = ec2_client.get_paginator("describe_instances")
    response_iterator = paginator.paginate(Filters=[{"Name": "private-ip-address", "Values": nodeaddr_list},],)
    filtered_iterator = response_iterator.search("Reservations[].Instances[].InstanceId")
    return [instance_id for instance_id in filtered_iterator]


def _resume_nodes(nodes):
    logging.info("Resuming nodes %s", nodes)
    resume_nodes_command = f"{SCONTROL} update nodename={nodes} state=resume"
    _run_command(resume_nodes_command, fail_on_error=False)


def suspend(arg_nodes):
    logging.debug("deleting nodes:" + arg_nodes)

    slurm_nodes = _get_nodes_info(arg_nodes)
    logging.info("slurm_nodes = %s", slurm_nodes)
    slurm_nodes = list(filter(lambda n: "DRAIN" not in n.state, slurm_nodes))
    logging.info("filtered slurm_nodes = %s", slurm_nodes)
    ec2_instance_ids = _get_instance_ids(list(map(lambda n: n.nodeaddr, slurm_nodes)))
    logging.info("ec2_instance_ids = %s", ec2_instance_ids)

    while True:
        delete_instances(ec2_instance_ids)
        if not len(retry_list):
            break

        logging.debug("got {} nodes to retry ({})".format(len(retry_list), ",".join(retry_list)))
        ec2_instance_ids = list(retry_list)
        del retry_list[:]

    time.sleep(10)
    _resume_nodes(arg_nodes)

    logging.debug("done deleting instances")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("nodes", help="Nodes to release")

    args = parser.parse_args()

    # silence module logging
    for logger in logging.Logger.manager.loggerDict:
        logging.getLogger(logger).setLevel(logging.WARNING)

    logging.basicConfig(filename=LOGFILE, format="%(asctime)s %(name)s %(levelname)s: %(message)s", level=logging.DEBUG)

    try:
        suspend(args.nodes)
    except Exception as e:
        logging.exception(e)


if __name__ == "__main__":
    main()
