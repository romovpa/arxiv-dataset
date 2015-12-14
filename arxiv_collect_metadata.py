#!/usr/bin/env python

import os
import time
import datetime
import logging
import argparse
import json

import pymongo
import requests
from lxml import etree
import dateutil.parser


logger = logging.getLogger(__name__)


def first(extracted, require=False):
    if len(extracted) > 0:
        return extracted[0]
    if require:
        raise RuntimeError('Value is not specified')


def parse_metadata_arXiv(record_element):
    ns = {
        'oai': 'http://www.openarchives.org/OAI/2.0/',
        'arxiv': 'http://arxiv.org/OAI/arXiv/',
    }
    
    header_element = record_element.find('oai:header', namespaces=ns)
    metadata_element = record_element.find('oai:metadata', namespaces=ns)
    if header_element is None or metadata_element is None:
        return
    
    arxiv_element = metadata_element.find('arxiv:arXiv', namespaces=ns)
    
    if arxiv_element is None:
        return
    
    oai_id = first(header_element.xpath('oai:identifier/text()', namespaces=ns))
    oai_datestamp = first(header_element.xpath('oai:datestamp/text()', namespaces=ns))
    oai_specs = [subel.text for subel in header_element.findall('oai:setSpec', namespaces=ns)]
    
    arxiv_id = first(arxiv_element.xpath('arxiv:id/text()', namespaces=ns), require=True)
    
    authors_element = arxiv_element.find('arxiv:authors', namespaces=ns)
    authors = []
    for author_element in authors_element.findall('arxiv:author', namespaces=ns):
        keyname = first(author_element.xpath('arxiv:keyname/text()', namespaces=ns))
        forenames = first(author_element.xpath('arxiv:forenames/text()', namespaces=ns))
        authors.append({
                'keyname': keyname, 
                'forenames': forenames,
                'name': ' '.join(author_element.xpath('*/text()', namespaces=ns)),
            })
    title = first(arxiv_element.xpath('arxiv:title/text()', namespaces=ns), require=True)
    abstract = first(arxiv_element.xpath('arxiv:abstract/text()', namespaces=ns), require=True).strip()
    categories = first(arxiv_element.xpath('arxiv:categories/text()', namespaces=ns), require=True)
    categories = filter(lambda s: len(s) > 0, categories.split(' ')) 
    
    info = {}
    for subelement in arxiv_element:
        tag = subelement.xpath('local-name()')
        if tag not in ('id', 'title', 'authors', 'categories', 'abstract'):
            info[tag] = subelement.text
    
    return {
        'oai_id': oai_id,
        'oai_datestamp': oai_datestamp,
        'oai_specs': oai_specs,
        
        'arxiv_id': arxiv_id,
        'title': title,
        'authors': authors,
        'categories': categories,
        'abstract': abstract,
        
        'info': info,
    }


def parse_metadata_arXivRaw(record_element):
    ns = {
        'oai': 'http://www.openarchives.org/OAI/2.0/',
        'arxiv': 'http://arxiv.org/OAI/arXivRaw/',
    }
    
    header_element = record_element.find('oai:header', namespaces=ns)
    metadata_element = record_element.find('oai:metadata', namespaces=ns)
    if header_element is None or metadata_element is None:
        return
    
    arxiv_element = metadata_element.find('arxiv:arXivRaw', namespaces=ns)
    if arxiv_element is None:
        return
    
    arxiv_id = first(arxiv_element.xpath('arxiv:id/text()', namespaces=ns), require=True)
    
    submitter = first(arxiv_element.xpath('arxiv:submitter/text()', namespaces=ns))
    
    versions = []
    for version_element in arxiv_element.findall('arxiv:version', namespaces=ns):
        version = version_element.attrib['version']
        
        date = first(version_element.xpath('arxiv:date/text()', namespaces=ns))
        size = first(version_element.xpath('arxiv:size/text()', namespaces=ns))
        date = dateutil.parser.parse(date).strftime('%Y-%m-%d %H:%M:%S')
        versions.append({'version': version, 'size': size, 'date': date})
    
    return {
        'arxiv_id': arxiv_id,
        'submitter': submitter,
        'versions': versions,
    }


def collect_metadata(metadata_collection, metadata_dir):
    
    """
    logger.info('Start reading arXiv metadata')
    
    next_metadata_file_id = 0
    while True:
        filename = os.path.join(
            metadata_dir, 
            'records_arXiv_%.10d.xml' % next_metadata_file_id,
        )

        if not os.path.exists(filename):
            break

        logger.info('Processing %s' % filename)

        tree = etree.parse(filename)
        record_elements = tree\
            .getroot()\
            .find('{http://www.openarchives.org/OAI/2.0/}ListRecords')\
            .findall('{http://www.openarchives.org/OAI/2.0/}record')

        metadata_objects = []
        for record_element in record_elements:
            obj = parse_metadata_arXiv(record_element)
            if obj is not None:
                obj['_id'] = obj.pop('arxiv_id')
                metadata_objects.append(obj)
            else:
                record_xml = etree.tostring(record_element)
                logger.error(
                    'Cannot parse metadata record'
                    'in {filename}: {record_xml}'.format(**locals())
                )

        metadata_collection.insert_many(metadata_objects)
        logger.info('Inserted %d records' % len(metadata_objects))

        next_metadata_file_id += 1

    """    
    logger.info('Start reading arXivRaw metadata')
    
    next_metadata_file_id = 0
    while True:
        filename = os.path.join(
            metadata_dir, 
            'records_arXivRaw_%.10d.xml' % next_metadata_file_id,
        )

        if not os.path.exists(filename):
            break

        logger.info('Processing %s' % filename)

        tree = etree.parse(filename)
        record_elements = tree\
            .getroot()\
            .find('{http://www.openarchives.org/OAI/2.0/}ListRecords')\
            .findall('{http://www.openarchives.org/OAI/2.0/}record')

        for record_element in record_elements:
            obj = parse_metadata_arXivRaw(record_element)
            if obj is not None:
                arxiv_id = obj.pop('arxiv_id')
                metadata_collection.update_one(
                    {'_id': arxiv_id},
                    {'$set': obj},
                )
            else:
                record_xml = etree.tostring(record_element)
                logger.error(
                    'Cannot parse raw metadata record'
                    'in {filename}: {record_xml}'.format(**locals())
                )

        next_metadata_file_id += 1


def write_to_jsonlines_file(metadata_collection, jsonlines_file):  
    logger.info('Writing metadata to jsonlines file')
    records = list(metadata_collection.find({}, {'_id': True, 'info.created': True}))
    
    logger.info('Sort records in jsonlines file by ???')
    records.sort(key=lambda record: '%s %s' % (record['info']['created'], record['_id']))
    
    for n, record in enumerate(records):
        metadata_record = metadata_collection.find_one(record['_id'])
        jsonlines_file.write(json.dumps(metadata_record, separators=(',', ':')) + '\n')
        if n % 1000 == 0 and n > 0:
            logger.info('Writed %d records' % n)
    
        
if __name__ == '__main__':
    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG,
    )

    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='mongodb://localhost:27017/arxiv')
    parser.add_argument('--drop-collection', default=False, action='store_true')
    parser.add_argument('--read-metadata-dir')
    parser.add_argument('--write-jsonlines-file', type=argparse.FileType('w'))
    args = parser.parse_args()
    
    client = pymongo.MongoClient(args.db)
    db_uri_parts = pymongo.uri_parser.parse_uri(args.db)
    db_name = db_uri_parts['database']
    collection_name = db_uri_parts['collection'] or 'metadata'

    arxiv_db = client[db_name]
    if args.drop_collection:
        arxiv_db.drop_collection(collection_name)    
    metadata_collection = arxiv_db[collection_name]
    
    if args.read_metadata_dir:
        collect_metadata(
            metadata_collection,
            metadata_dir=args.read_metadata_dir,
        )
    
    if args.write_jsonlines_file:
        write_to_jsonlines_file(
            metadata_collection,
            args.write_jsonlines_file,
        )
