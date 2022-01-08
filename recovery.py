"""recover.py

This tool should be used when Reflex needs to be backed up and restored
"""

import os
import json
import logging
import time
from datetime import datetime
from argparse import ArgumentParser
from typing import Any
from pyaml_env import parse_config


def backup(backup_path: str,  archive_password: str, archive_name: str, compress: bool) -> bool:
    '''
    Performs a backup of the reflex environment

    Parameters:
        backup_path (str): The directory where the raw JSON data should be placed
        archive_name (str): The name of the archive to compress backups into
        archive_password (str): When set the archive will be password protected
        compress (bool): Whether to compress the data in the backup folder

    Returns:
        True|False - Backup succeeded or didn't    
    '''

    backup_successful = False

    es = connections.get_connection()

    if not os.path.exists(backup_path):
        logging.info(f"Creating backup path {backup_path}")
        os.makedirs(backup_path)

    logging.info("Cleaning up the backup path")
    for f in os.listdir(backup_path):
        os.remove(os.path.join(backup_path, f))

    time.sleep(5)

    indices = ['-'.join(i.split('-')[:-1]) for i in es.indices.get('reflex-*')]

    for index in indices:
        query = {
            "query": {
                "match_all": {}
            },
            "size": "1000"
        }

        file_path = os.path.join(backup_path, f"{index}.json")

        docs = []

        
        for hit in scan(es, index=index, query=query):
            docs.append(hit["_source"])

        with open(file_path, 'w') as f:
            f.write(json.dumps(docs))

    return backup_successful


def document_stream(document: dict, index_name: str) -> dict:
    '''
    Creates a dictionary object that elasticsearch/opensearch streaming_bulk
    uses to insert documents into an index

    Parameters:
        document (dict): The source document to index
        index_name (str): The index to send the document to
    '''
    yield {
        "_index": index_name,
        "_type": "_doc",
        "_source": document
    }


def restore():
    raise NotImplementedError


def load_config(path="config.yml") -> dict:
    '''
    Loads the configuration file for the application
    and returns a configuration object for consumption in
    other areas

    Parameters:
        path (str): The path to the configuration file

    Returns:
        config (dict): A dictionary object containing configuration information
    '''
    config_error = False
    config = parse_config(path)

    return config


def build_es_connection(connections, hosts, username: str, password: str, use_ssl: bool, verify_names: bool, ca_cert: str) -> bool:
    '''
    Creates an elasticsearch/opensearch connection object

    Parameters:
        connections (connections): An elasticsearch-dsl/opensearch-dsl connections object
        hosts (str|list(str)): A list of elasticsearch hosts to connect to
        username (str): Username to authenticate with
        password (str): Password to authenticate with
        use_ssl (bool): Whether to connect over TLS or not
        verify_names (bool): Whether to valide hostnames
        ca_cert (str): The path to the CA certificate

    Return:
        bool
    '''

    elastic_connection = {
        'hosts': hosts,
        'verifiy_certs': verify_names,
        'use_ssl': use_ssl,
        'ssl_show_warn': False,
        'http_auth': (username, password)
    }

    if ca_cert not in ('', None):
        elastic_connection['ca_certs'] = ca_cert

    connections.create_connection(**elastic_connection)


if __name__ == "__main__":

    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    parser = ArgumentParser()
    parser.add_argument('--config', '-c', type=str,
                        help="The path to the configuration file", required=False)
    parser.add_argument('--mode', '-m', type=str,
                        help="The mode to run the script in", default='backup', choices=['backup', 'restore'], required=True)
    parser.add_argument('--backup-path', '-b', type=str,
                        help="The path to write backup information", required=False)
    parser.add_argument('--archive_name', '-a', type=str, help="The name of the archive, will default to reflex-YYYY-MM-DD.zip if not set",
                        default=f"reflex-{datetime.utcnow().strftime('%Y-%m-%d')}.zip", required=False)
    parser.add_argument('--archive-password', '-p', type=str,
                        help="The password to encrypt the backup archive with", required=False)
    parser.add_argument('--compress-backup', help="If flagged the backup files will be archived", required=False, default=False, action="store_true")
    parser.add_argument('--reflex-version', '-v', type=str,
                        help="The version of Reflex being worked on", required=False)
    parser.add_argument('--es-distro', help="The elasticsearch/opensearch disto",
                        required=False, choices=['elasticsearch', 'opensearch'])
    parser.add_argument(
        '--es-hosts', help="The elasticsearch/opensearch host to connect to", required=False)
    parser.add_argument(
        '--es-username', help="The username to connect to elasticsearch/opensearch", required=False)
    parser.add_argument(
        '--es-password', help="The password to connect to elasticsearch/opensearch", required=False)
    parser.add_argument(
        '--es-cacert', help="If using TLS for elasticsearch/opensearch provide the CA cert", default=None, required=False)
    parser.add_argument(
        '--es-use-tls', help="Whether to use TLS or not when connecting to elasticsearch/opensearch", default=False, action='store_true', required=False)
    parser.add_argument('--es-tls-verifynames',
                        help="If using TLS for elasticsearch/opensearch, verify hostnames", default=False, action='store_true', required=False)

    args = parser.parse_args()

    # Check if the user is loading from a configuration file
    if args.config:

        # Make sure the configuration file exists before trying to load it
        if os.path.exists(args.config):
            config = load_config(args.config)
        else:
            logging.error(f"Configuration file {args.config} not found.")
            exit(1)

        args.es_hosts = config['elasticsearch']['hosts']
        args.es_username = config['elasticsearch']['username']
        args.es_password = config['elasticsearch']['password']
        args.es_use_tls = config['elasticsearch']['use_ssl']
        args.es_tls_verifiynames = config['elasticsearch']['tls_verifynames']
        args.es_distro = config['elasticsearch']['distro']
        args.es_cacert = config['elasticsearch']['cacert']
        
        args.reflex_version = config['reflex']['version']

    # Import the tools we need from opensearch if es_distro is opensearch
    if args.es_distro == 'opensearch':
        from opensearchpy import OpenSearch as client
        from opensearchpy.helpers import streaming_bulk, scan
        from opensearch_dsl import Document, connections

    # Import the tools we need from elasticsearch if es_distro is elasticsearch
    if args.es_distro == 'elasticsearch':
        from elasticsearch import Elasticsearch as client
        from elasticsearch.helpers import streaming_bulk, scan
        from elasticsearch_dsl import Document, connections

    build_es_connection(connections, args.es_hosts, args.es_username,
                        args.es_password, args.es_use_tls, args.es_tls_verifynames, args.es_cacert)

    backup(args.backup_path, args.archive_password, args.archive_name, args.compress_backup)
