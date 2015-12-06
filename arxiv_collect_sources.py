#!/usr/bin/env python

import os
import re
import time
import datetime
import logging
import argparse
import tarfile

from lxml import etree

__all__ = [
    'read_manifest',
    'read_archives',
]


logger = logging.getLogger(__name__)


def read_manifest(manifest_filename):
    tree = etree.parse(manifest_filename)
    for file_el in tree.findall('file'):
        file_info = {
            subel.tag: subel.text
            for subel in file_el
        }
        yield file_info


        
re_item_filename_old = re.compile('(?P<partition>[^\d]+)(?P<id>\d+).(?P<ext>[^.]+)')
re_item_filename_new = re.compile('(?P<id>\d\d\d\d.\d+).(?P<ext>[^.]+)')

def parse_archive_item_filename(filename):
    m_old = re_item_filename_old.match(filename)
    m_new = re_item_filename_new.match(filename)

    if m_old is not None:
        arxiv_id = m_old.group('partition') + '/' + m_old.group('id')
        ext = m_old.group('ext')
        return arxiv_id, ext
     elif m_new is not None:
        arxiv_id = m_new.group('id')
        ext = m_new.group('ext')
        return arxiv_id, ext

        
def read_archives(manifest_filename, data_path):
    total_items = 0
    missing_items = 0
    item_records = []
    
    for file_info in read_manifest(manifest_filename):
        archive_filename = os.path.join(data_path, file_info['filename'])
        
        if not os.path.exists(archive_filename):
            logger.info('%s: - n/a -' % archive_filename)
            continue

        
def load_arxiv_items(arxiv_path='/home/romovpa/arxiv/', metadata_collection=None):
    total_items = 0
    missing_items = 0
    item_records = []

    re_item_filename_old = re.compile('(?P<partition>[^\d]+)(?P<id>\d+).(?P<ext>[^.]+)')
    re_item_filename_new = re.compile('(?P<id>\d\d\d\d.\d+).(?P<ext>[^.]+)')

    for file_info in read_manifest(os.path.join(arxiv_path, 'arXiv_src_manifest.xml')):
        archive_filename = os.path.join(arxiv_path, file_info['filename'])

        if not os.path.exists(archive_filename):
            print '%s: - n/a -' % archive_filename
            continue

        cur_total_items = int(file_info['num_items'])
        cur_processed_items = 0

        arch = tarfile.open(archive_filename)
        for member in arch.getmembers():
            if member.isfile:
                item_file_name = os.path.split(member.name)[1]

                m_old = re_item_filename_old.match(item_file_name)
                m_new = re_item_filename_new.match(item_file_name)
                item_record = None
                
                if m_old is not None:
                    item_record = {
                            'partition': m_old.group('partition'),
                            'id': m_old.group('partition') + '/' + m_old.group('id'),
                            'ext': m_old.group('ext'),
                            'source': file_info['filename'],
                            'month': (
                                ('19' if file_info['yymm'].startswith('9') else '20') + file_info['yymm']
                            ),
                        }
                elif m_new is not None:
                    item_record = {
                            'partition': None,
                            'id': m_new.group('id'),
                            'ext': m_new.group('ext'),
                            'source': file_info['filename'],
                            'month': (
                                ('19' if file_info['yymm'].startswith('9') else '20') + file_info['yymm']
                            ),
                        }
                
                if item_record is not None:
                    item_records.append(item_record)
                    cur_processed_items += 1
                    
                    if metadata_collection is not None:
                        metadata_collection.update_one(
                            {'_id': item_record['id']},
                            {'$set': {
                                    'content.src': {
                                        'archive': file_info['filename'],
                                        'path': member.name,
                                        'size': member.size,
                                        'ext': item_record['ext'],
                                    }
                                }},
                        )

        total_items += cur_total_items
        missing_items += cur_total_items - cur_processed_items

        if cur_processed_items < cur_total_items:
            print '%s: MISSING %d' % (archive_filename, cur_total_items-cur_processed_items)
        elif cur_processed_items > cur_total_items:
            print '%s: EXTRA %d' % (archive_filename, cur_processed_items-cur_total_items)
        else:
            print '%s: ok' % archive_filename


    print 'Missing documents: %d ' % missing_items
    
    return item_records
        
        
if __name__ == '__main__':
    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
    )
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', default='.')
    parser.add_argument('--metadata-dir', default='metadata')
    parser.add_argument('--db', default='mongodb://localhost:27017/arxiv')
    args = parser.parse_args()
    
    # source archives -> mongodb (basic info)
    
    