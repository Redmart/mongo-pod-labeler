#!/usr/bin/python3
import argparse
import logging

import pymongo
import time
from kubernetes import config, client
from pymongo.errors import ConnectionFailure


def get_mongo_client(hostname, port=27017):
    logging.debug(f"Connecting to mongo {hostname} ", )
    return pymongo.MongoClient(host=hostname, port=port)


def is_master(mongo_client):
    try:
        logging.debug(f"checking whether mongo is primary or secondary.", )
        return bool(mongo_client.admin.command("ismaster")["ismaster"])
    except ConnectionFailure:
        logging.error("Server not available")


def get_mongo_pods(k8s_api):
    mongo_host_names = []
    logging.debug(f"getting mongo pods for matched label(s) {args.pod_selector} in namespace {args.namespace}")
    pod_details = k8s_api.list_namespaced_pod(namespace="{}".format(args.namespace),
                                              label_selector="{}".format(args.pod_selector))
    for item in pod_details.items:
        mongo_host_names.append(item.metadata.name + "." + item.metadata.generate_name[:-1] + "." + item.metadata.namespace)
    return mongo_host_names


def label_mongo_pods(k8s_api, pod_name, labels):
    logging.info(f"applying label {labels.get('role')} to {pod_name}")
    return k8s_api.patch_namespaced_pod(name=pod_name, namespace="{}".format(args.namespace), body=labels)


def generate_pod_label_body(labels):
    patch_content = {"kind": "Pod", "apiVersion": "v1", "metadata": labels}
    return patch_content


def find_mongo_and_label(v1):
    pods = get_mongo_pods(v1)
    for pod in pods:
        my_client = get_mongo_client(pod, 27017)
        if is_master(my_client):
            mongo_role = "primary"
        else:
            mongo_role = "secondary"
        label_mongo_pods(v1, pod, generate_pod_label_body({"redmart.com/mongo-role": mongo_role}))


# MAIN
parser = argparse.ArgumentParser(description="Checking mongo pods and labelling them with primary/secondary accordingly")
parser.add_argument('--dry-run', dest='dry_run', action='store_true', default=False)
parser.add_argument('--namespace', dest='namespace', required=False, default='mongo')
parser.add_argument('--pod-selector', dest='pod_selector', default='app=mongodb-replicaset', required=False)
parser.add_argument('--config-file', dest='config_file', required=False)
parser.add_argument('--incluster-config', dest='incluster_config', action='store_true', required=False, default=False)
parser.add_argument('--insecure-skip-tls-verify', dest='skip_tls_verify', action='store_true', required=False, default=False)
parser.add_argument('--verbose', dest='verbose', action='store_true', required=False, default=False)
parser.add_argument('--update-period', dest='sleep_seconds', required=False, default=60)

args = parser.parse_args()

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.DEBUG if args.verbose else logging.INFO
)

logging.captureWarnings(True)
logging.info("Starting mongo replica labeler...")
logging.info(f"Dry run: {args.dry_run}")

if args.config_file is None:
    logging.info("Loading current kubernetes cluster config")
    config.load_incluster_config()
else:
    logging.info("Loading kubernetes from passed config file")
    config.load_kube_config(config_file=args.config_file)

logging.info(f"SSL Verify: {not args.skip_tls_verify}")
if args.skip_tls_verify:
    conf = client.Configuration()
    conf.verify_ssl = False
    conf.debug = False
    client.Configuration.set_default(conf)

v1Api = client.CoreV1Api()

while True:
    find_mongo_and_label(v1Api)
    logging.debug(f"Sleeping {args.sleep_seconds}...")
    time.sleep(args.sleep_seconds)
