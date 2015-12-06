#!/usr/bin/env python

import os
import time
import datetime
import logging
import argparse

import requests
from lxml import etree

logger = logging.getLogger(__name__)


def read_metadata_resumption_token(filename):
    tree = etree.parse(filename)
    resumption_token_element = tree\
        .getroot()\
        .find('{http://www.openarchives.org/OAI/2.0/}ListRecords')\
        .find('{http://www.openarchives.org/OAI/2.0/}resumptionToken')

    cursor = int(resumption_token_element.attrib['cursor'])
    complete_list_size = int(resumption_token_element.attrib['completeListSize'])
    token = resumption_token_element.text
    if token is not None and len(token) == 0:
        token = None
    return token, cursor, complete_list_size


def metadata_output_file_name(metadata_prefix, id):
    return 'records_%s_%.10d.xml' % (metadata_prefix, id)
    

def download_arxiv_metadata(metadata_files_path, oai_url, metadata_prefix, sleep_seconds):
    resumption_token = None
    next_metadata_file_id = 0

    while next_metadata_file_id == 0 or resumption_token is not None:
        
        next_metadata_file = os.path.join(
            metadata_files_path, 
            metadata_output_file_name(metadata_prefix, next_metadata_file_id),
        )

        if not os.path.exists(next_metadata_file):
            params = {'verb': 'ListRecords'}
            if resumption_token is None:
                logging.info('Starting new OAI chain')
                params['metadataPrefix'] = metadata_prefix
            else:
                params['resumptionToken'] = resumption_token

            try:
                resp = requests.get(oai_url, params=params, timeout=100)
            except KeyboardInterrupt:
                logging.error('Keyboard interrupt, stopping')
                return
            except requests.exceptions.Timeout:
                logging.error('Timeout, trying again')
                continue
            except requests.exceptions.ConnectionError as e:
                logging.error('Connection error: %s' % e.message)
                logging.error('Trying again')
                continue
            except Exception as e:
                logging.error('Exception: %s' % str(e))
                logging.error('Trying again')
                continue
                

            if not resp.ok:
                now = datetime.datetime.now()
                logging.error('HTTP status %d, sleeping' % resp.status_code)
                time.sleep(sleep_seconds)
                continue

            with open(next_metadata_file, 'w') as f:
                f.write(resp.content)

                
            resumption_token, cursor, complete_list_size = read_metadata_resumption_token(next_metadata_file)

            logging.info(
                'Downloaded {next_metadata_file}: cursor={cursor}, size={complete_list_size}, token={resumption_token}'.format(**locals())
            )
            
            time.sleep(sleep_seconds)
            
        else:
            logging.info('Skip {next_metadata_file}'.format(**locals()))
            resumption_token, cursor, complete_list_size = read_metadata_resumption_token(next_metadata_file)

        next_metadata_file_id += 1
        
    if next_metadata_file_id > 0 and resumption_token is None:
        logging.info('Empty resumptionToken, we are finished!')


if __name__ == '__main__':
    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
    )
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--sleep', default=25)
    parser.add_argument('--oai-url', default='http://export.arxiv.org/oai2')
    parser.add_argument('--metadata-prefix', default='arXiv')
    parser.add_argument('--output-dir', default='metadata')
    args = parser.parse_args()

    download_arxiv_metadata(
        metadata_files_path=os.path.abspath(args.output_dir),
        oai_url=args.oai_url,
        metadata_prefix=args.metadata_prefix,
        sleep_seconds=args.sleep,
    )