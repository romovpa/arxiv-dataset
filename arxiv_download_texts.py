#!/usr/bin/env python
"""
Create plain text dataset of arXiv papers, using PDF archives.

Requirements:
  - pip install boto requests lxml
  - apt-get install poppler-utils
"""

import os
import sys
import shutil
import tarfile
import tempfile
import time
import datetime
import logging
import argparse
import subprocess

import boto
import requests
from lxml import etree

logger = logging.getLogger(__name__)


def setup_logging(args):
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
    
    log_formatter = logging.Formatter(
        fmt='[%(asctime)s] %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)
    
    if args.log is not None:
        log_file_handler = logging.FileHandler(args.log)
        log_file_handler.setFormatter(log_formatter)
        logger.addHandler(log_file_handler)


def read_manifest(manifest_filename):
    manifest_records = []
    tree = etree.parse(manifest_filename)
    for file_el in tree.findall('file'):
        file_info = {
            subel.tag: subel.text
            for subel in file_el
        }
        for field in ('size', 'seq_num', 'num_items'):
            file_info[field] = int(file_info[field])
            
        yymm = file_info.pop('yymm')
        file_info['month'] = ('19' if yymm[0] == '9' else '20') + yymm
        manifest_records.append(file_info)
    return manifest_records


def filter_archives(manifest_records, start_month, finish_month):
    filtered_records = []
    for record in manifest_records:
        month = record['month']
        if (start_month is None or month >= start_month) and (finish_month is None or month <= finish_month):
            filtered_records.append(record)
    return filtered_records


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log')
    parser.add_argument('--debug', default=False, action='store_true')
    parser.add_argument('--txt-dir', default='txt')
    parser.add_argument('--pdf-dir', default='pdf')
    parser.add_argument('--error-pdf-dir', default='error_pdf')
    parser.add_argument('--remove-processed', default=False, action='store_true')
    parser.add_argument('--list', default=False, action='store_true')
    parser.add_argument('-s', '--start-month')
    parser.add_argument('-f', '--finish-month')
    args = parser.parse_args()
    
    setup_logging(args)
    
    s3 = boto.connect_s3()
    s3_headers={'x-amz-request-payer': 'requester'}
    arxiv_bucket = s3.get_bucket('arxiv', headers=s3_headers)
    
    logger.info('Downloading arXiv_pdf_manifest.xml from arxiv bucket')
    pdf_manifest_key = arxiv_bucket.get_key('pdf/arXiv_pdf_manifest.xml', headers=s3_headers)
    pdf_manifest_key.get_contents_to_filename('arXiv_pdf_manifest.xml', headers=s3_headers)
    manifest_records = read_manifest('arXiv_pdf_manifest.xml')
    
    selected_archives = filter_archives(manifest_records, args.start_month, args.finish_month)
    selected_archives.sort(key=lambda record: record['month'])
    
    if args.list:
        print 'Archives to process:'
        total_size = 0
        for archive in selected_archives:
            size_mb = archive['size'] / 1024**2
            print '    {filename}    {size_mb} Mb'.format(size_mb=size_mb, **archive)
            total_size += archive['size']
        print 'Total %d Mb' % (total_size / 1024**2)
        sys.exit(0)
    
    if not os.path.exists(args.pdf_dir):
        os.mkdir(args.pdf_dir)
    if not os.path.exists(args.txt_dir):
        os.mkdir(args.txt_dir)
    if not os.path.exists(args.error_pdf_dir):
        os.mkdir(args.error_pdf_dir)
    
    for archive in selected_archives:
        archive_name = os.path.split(archive['filename'])[1]
        
        # get archive from bucket
        archive_local_path = os.path.join(args.pdf_dir, archive_name)
        if not os.path.exists(archive_local_path):
            logger.info('%s: downloading from arxiv bucket' % archive_name)
            archive_key = arxiv_bucket.get_key(archive['filename'], headers=s3_headers)
            archive_key.get_contents_to_filename(archive_local_path, headers=s3_headers)
        else:
            logger.info('%s: local archive found' % archive_name)
            
        # extract archive contents
        tmp_dir = tempfile.mkdtemp('_arxiv_pdf_%s' % archive_name)
        logger.info('%s: extracting to %s' % (archive_name, tmp_dir))
        archive_pdf_files = []
        with tarfile.open(archive_local_path) as tf:
            tf.extractall(tmp_dir)
            for member in tf.getmembers():
                _, member_name = os.path.split(member.name)
                if member_name.endswith('.pdf'):
                    pdf_name, _ = os.path.splitext(member_name)
                    archive_pdf_files.append((pdf_name, member.name))
                    
        # convert archive contents to text
        logger.info('%s: converting to text' % archive_name)
        for pdf_name, archive_file in archive_pdf_files:
            pdf_path = os.path.join(tmp_dir, archive_file)
            txt_path = os.path.join(args.txt_dir, pdf_name + '.txt')
            try:
                cmd = ['pdftotext', '-enc', 'UTF-8', pdf_path, txt_path]
                if args.debug:
                    logger.debug('Running pdftotext: ' + str(cmd))
                subprocess.check_call(cmd)
                if args.debug:
                    logger.debug('Successfully converted %s to %s' % (pdf_name, txt_path))
            except subprocess.CalledProcessError as e:
                logger.error('Cannot convert PDF: %s from %s' % (pdf_name, archive_name))
                
                if args.error_pdf_dir:
                    shutil.copy(pdf_path, args.error_pdf_dir)

        logger.debug('Removing temp dir %s' % tmp_dir)
        shutil.rmtree(tmp_dir)
        
        if args.remove_processed:
            logger.info('Removing processed archive %s' % archive_local_path)
            os.remove(archive_local_path)

    logger.info('Finished')

