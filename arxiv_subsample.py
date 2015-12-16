#!/usr/bin/env python

import os
import sys
import logging
import argparse
import random
import shutil
import json


logger = logging.getLogger(__name__)


def iterate_arxiv_items(args):
    if args.metadata is not None:
        # read from file
        import json
        
        file_reader = open
        if args.metadata.endswith('.bz2'):
            import bz2
            file_reader = bz2.BZ2File
        elif args.metadata.endswith('.gz'):
            import gzip
            file_reader = gzip.GzipFile
        
        metadata_file = file_reader(args.metadata)
        for line in metadata_file:
            item = json.loads(line)
            yield item
            
    else:
        # read from mongodb
        import pymongo
        
        client = pymongo.MongoClient(args.db)
        db_uri_parts = pymongo.uri_parser.parse_uri(args.db)
        db_name = db_uri_parts['database']
        collection_name = db_uri_parts['collection'] or 'metadata'
        
        arxiv_db = client[db_name]
        metadata_collection = arxiv_db[collection_name]
        for item in metadata_collection:
            yield metadata_collection
    
    
if __name__ == '__main__':    
    logger.setLevel(logging.DEBUG)
    log_formatter = logging.Formatter(
        fmt='[%(asctime)s] %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='mongodb://localhost:27017/arxiv')
    parser.add_argument('--metadata')
    parser.add_argument('--txt-dir', default='txt')
    parser.add_argument('--output-dir')
    parser.add_argument('--subsample-rate', type=float)
    parser.add_argument('--start-date')
    parser.add_argument('--finish-date')
    args = parser.parse_args()
    
    if not os.path.exists(args.output_dir):
        os.makedirs(os.path.join(args.output_dir, 'txt'))

    metadata_subsample_file = open(os.path.join(args.output_dir, 'metadata.jsonlines'), 'w')

    n_processed = 0
    n_selected = 0
    n_texts = 0

    for item in iterate_arxiv_items(args):
        if n_processed % 20000 == 0 and n_processed > 0:
            logger.info('Processed %d items, current_date: %s, selected: %d (%.2f%%), text coverage: %d (%.2f%%)' % (
                        n_processed,
                        created_date,
                        n_selected,
                        float(n_selected) / n_processed * 100,
                        n_texts,
                        float(n_texts) / (n_selected + 1e-10) * 100,
                    ))

        n_processed += 1

        if args.subsample_rate is not None:
            random_number = random.random()
            if random_number > args.subsample_rate:
                continue

        created_date = item['info'].get('created')
        if args.start_date is not None and (created_date is None or created_date < args.start_date):
            continue
        if args.finish_date is not None and (created_date is None or created_date > args.finish_date):
            continue

        n_selected += 1

        metadata_subsample_file.write(
            json.dumps(item, separators=(',', ':')) + '\n'
        )

        txt_name = item['_id'].replace('/', '') + '.txt'
        txt_file = os.path.join(args.txt_dir, txt_name)
        new_txt_file = os.path.join(args.output_dir, 'txt', txt_name)
        if os.path.exists(txt_file):
            n_texts += 1
            shutil.copy(txt_file, new_txt_file)

    metadata_subsample_file.close()
    
    logger.info('Finished. Selected %d of %d (%.2f%%), text coverage: %d (%.2f%%)' % (
            n_selected,
            n_processed,
            float(n_selected) / n_processed * 100,
            n_texts,
            float(n_texts) / (n_selected + 1e-10) * 100,
        ))
