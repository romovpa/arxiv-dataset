#!/usr/bin/env python

import os
import sys
import logging
import argparse
import string

import nltk


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
    
    

class SuperTokenizer(object):
    UNICODE_CHARS_MAP = {
        u'\u2018': '\'',
        u'\u2019': '\'',
        u'\u2013': '-',
        u'\u201c': '"',
        u'\xa8': '',
        u'/': ' ',
    }
    
    def __init__(self):
        self.english_stopwords = nltk.corpus.stopwords.words('english')
        self.wordnet_lemmatizer = nltk.WordNetLemmatizer()
        
    def tokenize(self, content):

        # replace unicode symbols with ascii analogs
        content_processed = content
        for ch, ch_replacement in SuperTokenizer.UNICODE_CHARS_MAP.iteritems():
            content_processed = content_processed.replace(ch, ch_replacement)
            
        # apply nltk's best tokenizer
        tokens = nltk.word_tokenize(content_processed)

        # map to lower case
        tokens = map(lambda s: s.lower(), tokens)
        
        # filter short tokens
        tokens = filter(lambda s: len(s) >= 2, tokens)
        
        # filter stopwords
        tokens = filter(lambda w: w not in self.english_stopwords, tokens)

        # strip punctuation
        tokens = map(lambda w: w.strip(string.punctuation), tokens)
        
        # lemmatize
        tokens = [self.wordnet_lemmatizer.lemmatize(t) for t in tokens]
        
        return tokens


def load_content(item, args):
    txt_filename = os.path.join(args.txt_dir, item['_id'].replace('/', '') + '.txt')
    if os.path.exists(txt_filename):
        with open(txt_filename) as txt_file:
            return txt_file.read().decode('utf8')
    

def extract_features(item, content, args):
    tokenizer = SuperTokenizer()
    
    namespaces = {}
    
    # create namespace tokens
    namespaces['title'] = tokenizer.tokenize(item['title'])
    namespaces['abstract'] = tokenizer.tokenize(item['abstract'])
    namespaces['authors'] = [
        author['keyname']
        for author in item['authors']
    ]
    namespaces['categories'] = item['categories']
    
    if content:
        namespaces['text'] = tokenizer.tokenize(content)
    
    def escape_token(word):
        return word.replace('|', '_').replace(' ', '_').replace(':', '_')
    
    return item['_id'] + ' ' + u''.join(
        (u'|%s %s ' % (ns, ' '.join(map(escape_token, tokens))))
        for ns, tokens in namespaces.iteritems()
    )   
    
    
    
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
    parser.add_argument('--output', type=argparse.FileType('w'), default=sys.stdout)
    args = parser.parse_args()
    
    for n, arxiv_item in enumerate(iterate_arxiv_items(args)):
        content = load_content(arxiv_item, args)
        line = extract_features(arxiv_item, content, args)
        args.output.write(line.encode('utf8') + '\n')
        if n % 100 == 0 and n > 0:
            logger.info('Processed %d items' % n)
