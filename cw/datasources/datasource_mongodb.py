
#!/opt/python27/bin/python
# -*- coding: utf-8 -*-

import pymongo
from datetime import datetime
import re
import traceback
import sys
import cw.datasources.datasource_default
from cw.models.html import Html
import urlparse

"""
host = '192.168.100.116'
port = 27017
db_name = 'xxxxx'
html_collection_name = 'htmls'
host_collection_name = 'hosts'
"""


class DataAccess(cw.datasources.datasource_default.DataAccess):
    def __init__(self,
                 host='localhost',
                 port=27017,
                 db_name='crawlerdb',
                 html_regex=r'text/.*',
                 prefix='',
                 option={
                 'html_collection_name': 'htmls',
                 'host_collection_name': 'hosts'
                 },
                 logger = None
                 ):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.db = pymongo.Connection(self.host, self.port)[self.db_name]
        self.prefix = prefix
        self.html_collection_name = self.prefix + '_' + option['html_collection_name']
        self.htmls = self.db[self.html_collection_name]
        self.host_collection_name = self.prefix + '_' + option['host_collection_name']
        self.hosts = self.db[self.host_collection_name]
        self.html_re = re.compile(html_regex, re.IGNORECASE)
        self.logger = logger

    def get_db(self):
        return pymongo.Connection(self.host, self.port)[self.db_name]

    def get_collection(self, collection_name):
        return self.db[collection_name]

    def get_next_url(self):
        doc = self.htmls.find_and_modify(
            query={'crawled_at': {'$exists': False}},
            update={'$set': {'crawled_at': datetime.now()}},
            upsert=False, sort={'priority': -1})
        if doc is None:
            return None
        obj = Html()
        obj.from_dict(doc)
        return obj

    def get_next_html(self):
        doc = self.htmls.find_and_modify(
            query={'crawled_at': {'$exists': True}, 'parsed_at': {'$exists': False}, 'content_type': self.html_re},
            update={'$set': {'parsed_at': datetime.now()}},
            upseart=False,
            sort={'priority': -1})
        if doc is None:
            return None
        obj = Html()
        obj.from_dict(doc)
        return obj

    def update_crawled_html(self, html):
        if not isinstance(html, Html):
            raise TypeError()

        if 'file_path' in dir(html):
            self.htmls.update({'md5hash': html.md5hash},
                              {'$set': {'file_path': html.file_path,
                                        'content_type': html.content_type,
                                        'response_code': html.response_code,
                                        'response_headers': html.response_headers,
                                        'request_headers': html.request_headers,
                                        'crawled_at': html.crawled_at,
                                        'parsed_at': html.parsed_at}})
        elif 'html' in dir(html):
            self.htmls.update({'md5hash': html.md5hash},
                              {'$set': {'html': html.html,
                                        'content_type': html.content_type,
                                        'response_code': html.response_code,
                                        'response_headers': html.response_headers,
                                        'request_headers': html.request_headers,
                                        'crawled_at': html.crawled_at}})
        else:
            try:
                self.htmls.update({'md5hash': html.md5hash},
                                  {'$set': {'content_type': html.content_type,
                                            'response_code': html.response_code,
                                            'response_headers': html.response_headers,
                                            'request_headers': html.request_headers,
                                            'crawled_at': html.crawled_at,
                                            'parsed_at': html.parsed_at}})
            except:
                self.logger.exception('error (url: %s) %s' % (html.url, html.to_json()))

    def update_parsed_html(self, html):
        self.htmls.update({'md5hash': html.md5hash},
                          {'$set': {'parsed_at': html.parsed_at,
                                    'destinations': html.destinations}})

    def insert_htmls(self, htmls):
        if len(htmls) == 0:
            return

        try:
            self.htmls.ensure_index('md5hash', unique=True)
            self.htmls.ensure_index('crawled_at')
            self.htmls.ensure_index('priority')
            #self.htmls.ensure_index([('priority', pymongo.DESCENDING)])
        except Exception, e:
            print '***********************************************************************************************'
            print e
            traceback.print_tb(sys.exc_info()[2])

        self.htmls.insert([html.to_dict() for html in htmls if 'url' in dir(html)], continue_on_error=True)

    def is_exists(self, md5hash):
        return self.htmls.find_one({'md5hash': md5hash}) is not None

    def get_destinations(self, urls):
        url_link_table, host_link_table = {}, {}
        for src, dst in [(url.referer, url.url) for url in urls if url.referer is not None and len(url.referer) > 0]:
            if src not in url_link_table:
                url_link_table[src] = []
            url_link_table[src].append(dst)
            src_host = urlparse.urlparse(src).hostname
            if src_host not in host_link_table:
                host_link_table[src_host] = []
            host_link_table[src_host].append(urlparse.urlparse(dst).hostname)
        return url_link_table, host_link_table

    def store_links(self, src, dsts):
        for dst in dsts:
            self.htmls.update({'md5hash': Html(url=src).md5hash},
                              {'$addToSet': {'destinations': dst}})

    def store_hosts(self, src, dsts):
        for dst in dsts:
            self.hosts.update({'source': src},
                              {'$addToSet': {'destinations': dst}},
                              upsert=True)

    def dump_htmls(self):
        result = ''

        for d in self.htmls.find():
            #print d.keys()
            result += Html().from_dict(d).to_line_string() + '\n'
        return result

    def get_url_count_by_priority(self, priority):
        return self.htmls.find(spec={'priority': {'$gte': priority}, 'crawled_at': {'$exists': True}}).count()

    def get_htmls(self):
        htmls = []
        for d in self.htmls.find():
            htmls.append(Html().from_dict(d))
        return htmls

    def get_crawled_urls(self):
        return self.htmls.find(spec={'crawled_at': {'$exists': True}}, fields=['md5hash'])

    def get_not_parsed_htmls(self):
        htmls = []
        for doc in self.htmls.find(spec={'crawled_at': {'$exists': True},
                                         'parsed_at': {'$exists': False}}):
            htmls.append(Html().from_dict(doc))
        return htmls

    def get_not_crawled_urls(self):
        urls = []
        for doc in self.htmls.find(spec={'crawled_at': {'$exists': False}}):
            urls.append(Html().from_dict(doc))
        return urls

    def set_property(self, md5hash, property_name, property_value):
        if self.htmls.find_one({'md5hash': md5hash}) is not None:
            self.htmls.update({'md5hash': md5hash}, {'$set': {property_name: property_value}})

