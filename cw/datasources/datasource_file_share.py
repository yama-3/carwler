
#!/opt/python27/bin/python
# -*- coding: utf-8 -*-

import re
import os
import os.path
import codecs
import cw.datasources.datasource_default
from cw.models.html import Html


class DataAccess(cw.datasources.datasource_default.DataAccess):
    def __init__(self,
                 html_regex=r'text/.*',
                 prefix='',
                 option={},
                 logger = None
                 ):
        self.prefix = prefix
        self.store_root = os.path.join(option['store_root'], self.prefix)
        if not os.path.exists(self.store_root):
            os.makedirs(self.store_root)
        self.html_re = re.compile(html_regex, re.IGNORECASE)
        self.logger = logger
        self.not_crawled_htmls = []
        self.crawled_htmls = []
        self.parsed_htmls = []

    def find_all_files(self):
        for root, dirs, files in os.walk(self.store_root):
            for file in files:
                if not os.path.splitext(file)[1] == '.json':
                    continue
                yield os.path.join(root, file)

    def get_htmls(self):
        for json_file in self.find_all_files():
            html = self.open(json_file)
            if html.url is None or len(html.url) == 0:
                self.logger.debug({'json_file': json_file, 'message': 'error html file'})
                continue
            yield html

    def open(self, file_path):
        with codecs.open(file_path, 'r', 'utf-8') as f:
            return Html(json_str=f.read())

    def build_filepath(self, md5hash):
        idx, inc = 0, 3
        file_path = self.store_root
        while (idx + inc) < 32:
            file_path = os.path.join(file_path, md5hash[idx:idx+inc])
            idx += inc
        file_path = os.path.join(file_path, md5hash + '.json')
        return file_path

    def is_exists(self, md5hash):
        file_path = self.build_filepath(md5hash)
        return os.path.exists(file_path)    

    def save(self, file_path, json_string):
        try:
            dir_path, file_name = os.path.split(file_path)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            with codecs.open(file_path, 'w', 'utf-8') as f:
                f.write(json_string)
        except:
            self.logger.exception('error (url: %s) %s' % (file_path, json_string))

    def update_crawled_html(self, html):
        if not isinstance(html, Html):
            raise TypeError()
        file_path = self.build_filepath(html.md5hash)
        self.save(file_path, html.to_json())

    def update_parsed_html(self, html):
        if not isinstance(html, Html):
            raise TypeError()
        file_path = self.build_filepath(html.md5hash)
        self.save(file_path, html.to_json())

    def insert_htmls(self, htmls):
        if len(htmls) == 0:
            return
        for html in htmls:
            file_path = self.build_filepath(html.md5hash)
            self.save(file_path, html.to_json())

    def store_links(self, src, dsts):
        json_file = self.build_filepath(Html(url=src).md5hash)
        if not os.path.exists(json_file):
            return
        html = self.open(json_file)
        destinations = set(html.destinations)
        for dst in dsts:
            if dst in destinations:
                continue
            destinations.add(dst)
        html.destinations = destinations
        self.save(json_file, html.to_json())

    def get_next_url(self):
        # called by crawler
        for json_file in self.find_all_files():
            html = self.open(json_file)
            if html.crawled_at is None:
                return html
        self.logger.debug({'message': 'not found json file'})
        return None

    def get_next_html(self):
        # called by parser
        for json_file in self.find_all_files():
            html = self.open(json_file)
            if html.crawled_at is not None and html.parsed_at is None:
                return html
        self.logger.debug({'message': 'not found json file'})
        return None

    def get_not_crawled_urls(self):
        urls = []
        for json_file in self.find_all_files():
            url = self.open(json_file)
            if url.crawled_at is None:
                urls.append(url)
        return urls

    def get_crawled_urls(self):
        urls = []
        for json_file in self.find_all_files():
            url = self.open(json_file)
            if url.crawled_at is not None:
                urls.append(url.md5hash)
        return urls

    def get_not_parsed_htmls(self):
        htmls = []
        for json_file in self.find_all_files():
            html = self.open(json_file)
            if html.crawled_at is not None and html.parsed_at is None:
                htmls.append(html)
        return htmls


    def dump_htmls(self):
        return ''

    def get_url_count_by_priority(self, priority):
        return 0

    def store_hosts(self, src, dsts):
        pass

    def get_destinations(self, urls):
        pass

    def set_property(self, md5hash, property_name, property_value):
        pass

