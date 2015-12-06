#!/usr/bin/env python

import os
import time
import datetime
import logging
import argparse

import pymongo
import requests
from lxml import etree


logger = logging.getLogger(__name__)


def parse_metadata_arXiv(record_element):
    
    def first(extracted, require=False):
        if len(extracted) > 0:
            return extracted[0]
        if require:
            raise RuntimeError('Value is not specified')

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
    

def collect_metadata(metadata_collection, metadata_dir):
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


if __name__ == '__main__':
    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
    )

    parser = argparse.ArgumentParser()
    parser.add_argument('--metadata-dir', default='metadata')
    parser.add_argument('--db', default='mongodb://localhost:27017/arxiv')
    parser.add_argument('--drop-collection', default=False, action='store_true')
    args = parser.parse_args()
    
    client = pymongo.MongoClient(args.db)
    db_uri_parts = pymongo.uri_parser.parse_uri(args.db)
    db_name = db_uri_parts['database']
    collection_name = db_uri_parts['collection'] or 'metadata'

    arxiv_db = client[db_name]
    if args.drop_collection:
        arxiv_db.drop_collection(collection_name)    
    metadata_collection = arxiv_db[collection_name]
    
    collect_metadata(
        metadata_collection,
        metadata_dir=args.metadata_dir,
    )
