#!/usr/local/pyenv/versions/slurm/bin/python

import argparse
import collections
import logging
import shlex
import subprocess
import boto3

### Manual steps
# 1. add to LaunchTemplate the subnet-id in the network interface
# 2. add policy to run new intances

REGION = "eu-west-1"
LAUNCH_TEMPLATE = {
    "LaunchTemplateId": "lt-06d29df441fc7fe83"
}
SUBNET_ID = "subnet-0f45a3009addd0e7f"
TAG_SPECIFICATIONS = [
    {
        "ResourceType": "instance",
        "Tags": [
            {
                "Key": "Name",
                "Value": "cloud-bursting-compute",
            }
        ]
    }
]

SCONTROL = '/opt/slurm/bin/scontrol'
LOGFILE = '/home/slurm/slurm_resume.log'
INSTANCES_BATCH_SIZE = 10

# Set to True if the nodes aren't accessible by dns.
UPDATE_NODE_ADDRS = True

EC2Instance = collections.namedtuple(
    "EC2Instance", ["id", "private_ip", "hostname"]
)


def run_command(command, capture_output=True, log_error=True, log_output=True, env=None):
    """Execute shell command."""
    if isinstance(command, str):
        command = shlex.split(command)
    logging.info("Executing command: " + " ".join(command))
    result = subprocess.run(command, capture_output=capture_output, universal_newlines=True, encoding="utf-8", env=env)
    try:
        result.check_returncode()
    except subprocess.CalledProcessError:
        if log_error:
            logging.error(
                "Command {0} failed with error:\n{1}\nand output:\n{2}".format(
                    " ".join(command), result.stderr, result.stdout
                )
            )
        raise

    if log_output:
        logging.debug("Output for command %s:\n%s", command, result.stdout)

    return result


def update_slurm_node_addrs(instances, slurm_nodes):
    for instance, slurm_node in zip(instances, slurm_nodes):
        node_update_cmd = "{} update node={} nodeaddr={} nodehostname={}".format(
            SCONTROL, slurm_node, instance.private_ip, instance.hostname)
        run_command(node_update_cmd)
        logging.info("Instance " + slurm_node + " is now configured")


def _parse_ec2_instance(instance):
    return EC2Instance(instance["InstanceId"],
                       private_ip=instance["PrivateIpAddress"],
                       hostname=instance["PrivateDnsName"].split(".")[0])


def add_instances(node_list):
    ec2_client = boto3.client('ec2', region_name=REGION)
    total = len(node_list)
    launched_instances = []
    while total > 0:
        batch_count = min(total, INSTANCES_BATCH_SIZE)
        result = ec2_client.run_instances(LaunchTemplate=LAUNCH_TEMPLATE,
                                          MinCount=batch_count,
                                          MaxCount=batch_count,
                                          InstanceType="c5.xlarge",
                                          TagSpecifications=TAG_SPECIFICATIONS)
        launched_instances.extend([_parse_ec2_instance(instance) for instance in result["Instances"]])
        total -= batch_count

    if UPDATE_NODE_ADDRS:
        update_slurm_node_addrs(launched_instances, node_list)


def main(arg_nodes):
    logging.debug("Bursting out:" + arg_nodes)

    # Get node list
    show_hostname_cmd = "{} show hostnames {}".format(SCONTROL, arg_nodes)
    nodes_str = run_command(show_hostname_cmd).stdout
    node_list = nodes_str.splitlines()

    retry_list = []
    while True:
        add_instances(node_list)
        if not len(retry_list):
            break

        logging.debug("got {} nodes to retry ({})".
                      format(len(retry_list), ",".join(retry_list)))
        node_list = list(retry_list)
        del retry_list[:]

    logging.debug("done adding instances")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('nodes', help='Nodes to burst')

    args = parser.parse_args()

    # silence module logging
    for logger in logging.Logger.manager.loggerDict:
        logging.getLogger(logger).setLevel(logging.WARNING)

    logging.basicConfig(
        filename=LOGFILE,
        format='%(asctime)s %(name)s %(levelname)s: %(message)s',
        level=logging.DEBUG)

    try:
        main(args.nodes)
    except Exception as e:
        logging.exception(e)
