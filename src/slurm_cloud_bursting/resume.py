import collections
import json
import logging
import shlex
import subprocess

import argparse
import boto3

REGION = "us-east-2"
CLUSTER_NAME = "hit"
TAG_SPECIFICATIONS = [{"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": "cloud-bursting-compute",}]}]

SCONTROL = "/opt/slurm/bin/scontrol"
LOGFILE = "/home/slurm/slurm_resume.log"
QUEUES_CONFIG = "/opt/parallelcluster/configs/slurm_config.json"
INSTANCES_BATCH_SIZE = 10

# Set to True if the nodes aren't accessible by dns.
UPDATE_NODE_ADDRS = True

EC2Instance = collections.namedtuple("EC2Instance", ["id", "private_ip", "hostname"])


def run_command(command, log_error=True, log_output=True, env=None):
    """Execute shell command."""
    if isinstance(command, str):
        command = shlex.split(command)
    logging.info("Executing command: " + " ".join(command))
    result = subprocess.run(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, encoding="utf-8", env=env
    )
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


def update_slurm_node_addrs(instances):
    for instance, slurm_node in instances:
        node_update_cmd = "{} update node={} nodeaddr={} nodehostname={}".format(
            SCONTROL, slurm_node, instance.private_ip, instance.hostname
        )
        run_command(node_update_cmd)
        logging.info("Instance " + slurm_node + " is now configured")


def _parse_ec2_instance(instance):
    return EC2Instance(
        instance["InstanceId"],
        private_ip=instance["PrivateIpAddress"],
        hostname=instance["PrivateDnsName"].split(".")[0],
    )


def add_instances(node_list, queues_config):
    ec2_client = boto3.client("ec2", region_name=REGION)

    instances_to_launch = collections.defaultdict(lambda: collections.defaultdict(list))
    for node in node_list:
        queue_name = node.split("-")[0]
        instance_type = node.split("-")[2]
        instances_to_launch[queue_name][instance_type].append(node)

    launched_instances = []
    for queue, queue_instances in instances_to_launch.items():
        for instance_type, instances_list in queue_instances.items():
            total = len(instances_list)
            while total > 0:
                batch_count = min(total, INSTANCES_BATCH_SIZE)
                run_instances_args = {}
                if queues_config[queue].get("disable_hyperthreading", False):
                    core_count = next(
                        (
                            instance["vcpus"]
                            for instance in queues_config[queue]["instances"]
                            if instance["type"] == instance_type
                        )
                    )
                    run_instances_args["CpuOptions"] = {"CoreCount": core_count, "ThreadsPerCore": 1}
                # TODO add spot_price to run_instances_args

                result = ec2_client.run_instances(
                    LaunchTemplate={"LaunchTemplateName": CLUSTER_NAME + "-" + queue},
                    MinCount=batch_count,
                    MaxCount=batch_count,
                    InstanceType=instance_type,
                    TagSpecifications=TAG_SPECIFICATIONS,
                    **run_instances_args
                )
                launched_instances.extend(
                    [
                        (_parse_ec2_instance(ec2_instance), slurm_instance)
                        for ec2_instance, slurm_instance in zip(result["Instances"], instances_list)
                    ]
                )
                total -= batch_count

    if UPDATE_NODE_ADDRS:
        update_slurm_node_addrs(launched_instances)


def resume(arg_nodes):
    logging.debug("Bursting out:" + arg_nodes)

    # Get node list
    show_hostname_cmd = "{} show hostnames {}".format(SCONTROL, arg_nodes)
    nodes_str = run_command(show_hostname_cmd).stdout
    node_list = nodes_str.splitlines()

    with open(QUEUES_CONFIG) as f:
        queues_config = json.load(f)["queues_config"]

    retry_list = []
    while True:
        add_instances(node_list, queues_config)
        if not len(retry_list):
            break

        logging.debug("got {} nodes to retry ({})".format(len(retry_list), ",".join(retry_list)))
        node_list = list(retry_list)
        del retry_list[:]

    logging.debug("done adding instances")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("nodes", help="Nodes to burst")

    args = parser.parse_args()

    # silence module logging
    for logger in logging.Logger.manager.loggerDict:
        logging.getLogger(logger).setLevel(logging.WARNING)

    logging.basicConfig(filename=LOGFILE, format="%(asctime)s %(name)s %(levelname)s: %(message)s", level=logging.DEBUG)

    try:
        resume(args.nodes)
    except Exception as e:
        logging.exception(e)


if __name__ == "__main__":
    main()
