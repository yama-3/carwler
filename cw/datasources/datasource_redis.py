
# -*- coding: utf-8 -*-

import redis
from cw.models.html import Html
import cw.datasources.datasource_default
from datetime import datetime
import re


"""
host = 127.0.0.1
port = 6379
key_prefix = 'hoge'
"""

url_rank = "url_rank"
url_unique = "url_unique"
html_rank = "html_rank"
html_parsed = "html_parsed"


class DataAccess(cw.datasources.datasource_default.DataAccess):
    def __init__(self,
                 host='localhost',
                 port=6379,
                 html_regex=r'text/*',
                 prefix='',
                 option={},
                 logger=None
                 ):
        self.prefix = prefix + '_'
        self.url_rank = self.prefix + url_rank
        self.url_unique = self.prefix + url_unique
        self.html_rank = self.prefix + html_rank
        self.html_parsed = self.prefix + html_parsed
        self.html_regex = re.compile(html_regex)
        self.logger = logger

        self.cli = redis.Redis(host=host, port=port)
        if self.cli.exists(self.url_rank):
            raise Exception('DataAccessor.__init__', '{0} is exits'.format(self.url_rank))
        #if self.cli.exists(self.url_unique):
        #    raise Exception('DataAccessor.__init__', '{0} is exits'.format(self.url_unique))
        if self.cli.exists(self.html_rank):
            raise Exception('DataAccessor.__init__', '{0} is exits'.format(self.html_rank))
        if self.cli.exists(self.html_parsed):
            raise Exception('DataAccessor.__init__', '{0} is exits'.format(self.html_parsed))

    def get_next_url(self):
        if self.cli.zcard(self.url_rank) == 0:
            return None
        with self.cli.pipeline() as p:
            while True:
                try:
                    p.watch('get_next_url')
                    md5hash_list = p.zrevrange(self.url_rank, 0, 0)
                    if len(md5hash_list) == 0:
                        continue
                    md5hash = md5hash_list[0]
                    url_json = p.get(self.prefix + md5hash)
                    p.multi()
                    p.zrem(self.url_rank, md5hash)
                    p.execute()
                    html = Html(json_str=url_json)
                    html.crawled_at = datetime.now()
                    return html
                except redis.WatchError:
                    self.logger.exception({'datasource': 'redis', 'message': 'redis.WatchError', 'method': 'get_next_url'})
                    continue
                except:
                    self.logger.exception({'datasource': 'redis', 'message': 'Exception', 'method': 'get_next_url'})
                    continue

    def get_next_html(self):
        if self.cli.zcard(self.html_rank) == 0:
            return None
        with self.cli.pipeline() as p:
            while True:
                try:
                    p.watch('get_next_html')
                    md5hash_list = p.zrevrange(self.html_rank, 0, 0)
                    if len(md5hash_list) == 0:
                        return None
                    md5hash = md5hash_list[0]
                    html_json = p.get(self.prefix + md5hash)
                    p.multi()
                    p.zrem(self.html_rank, md5hash)
                    p.execute()
                    html = Html(json_str=html_json)
                    if html.url is None or len(html.url) == 0:
                        continue
                    return html
                except redis.WatchError:
                    self.logger.exception({'datasource': 'redis', 'message': 'redis.WatchError', 'method': 'get_next_html'})
                    continue
                except:
                    self.logger.exception({'datasource': 'redis', 'message': 'Exception', 'method': 'get_next_url'})
                    continue

    def update_crawled_html(self, html):
        """
        called by Crawler
        """
        if html is None or not isinstance(html, Html):
            return

        with self.cli.pipeline() as p:
            while True:
                try:
                    p.watch('update_crawled_html')
                    p.multi()
                    p.set(self.prefix + html.md5hash, html.to_json())
                    if 'html' not in dir(html) or html.html is None or len(html.html) == 0:
                        # パースできない html インスタンス
                        p.sadd(self.html_parsed, html.md5hash)
                    if self.html_regex.match(html.content_type) is not None:
                        p.zadd(self.html_rank, html.md5hash, html.priority)
                    p.execute()
                    break
                except redis.WatchError:
                    continue

    def update_parsed_html(self, html):
        with self.cli.pipeline() as p:
            while True:
                try:
                    p.watch('update_parsed_html')
                    p.multi()
                    p.set(self.prefix + html.md5hash, html.to_json())
                    p.sadd(self.html_parsed, html.md5hash)
                    p.execute()
                    break
                except redis.WatchError:
                    continue

    def insert_htmls(self, htmls):
        """
        called by Parser
        """
        self.logger.debug({'htmls count': len(htmls)})
        filtered_htmls = [html for html in htmls if isinstance(html, Html) and 'url' in dir(html)]
        self.logger.debug({'filtered htmls count': len(filtered_htmls)})
        for html in filtered_htmls:
            self.logger.debug({'insert_html': html.url})
            with self.cli.pipeline() as p:
                while True:
                    try:
                        p.watch('insert_htmls_%s' % html.md5hash)
                        if p.sismember(self.html_parsed, html.md5hash):
                            self.logger.debug({'message': '%s is exists.' % (html.url)})
                            break
                        p.multi()
                        p.sadd(self.url_unique, html.md5hash)
                        if p.zadd(self.url_rank, html.md5hash, html.priority) == 0:
                            break
                        p.set(self.prefix + html.md5hash, html.to_json())
                        p.execute()
                        break
                    except redis.WatchError:
                        continue

    def store_host(self, src_host, dst_hosts):
        pass

    def get_parsed_htmls(self, count):
        htmls = []
        for i in range(0, count):
            with self.cli.pipeline() as p:
                try:
                    p.watch('get_parsed_htmls')
                    md5hash = p.spop(self.html_parsed)
                    if not md5hash:
                        break
                    key = self.prefix + md5hash
                    html = Html(json_str=p.get(key))
                    p.multi()
                    p.delete(key)
                    p.execute()
                    htmls.append(html)
                except redis.WatchError:
                    continue
        return htmls

    def get_not_parsed_htmls(self):
        htmls = []
        while True:
            with self.cli.pipeline() as p:
                try:
                    p.watch('get_not_parsed_htmls')
                    md5hash_list = p.zrange(self.html_rank, 0, 0)
                    if len(md5hash_list) == 0:
                        break
                    md5hash = md5hash_list[0]
                    key = self.prefix + md5hash
                    html = Html(json_str=p.get(key))
                    p.multi()
                    p.delete(key)
                    p.zrem(self.html_rank, md5hash)
                    p.execute()
                    htmls.append(html)
                except redis.WatchError:
                    continue
        return htmls

    def get_not_crawled_urls(self):
        htmls = []
        while True:
            with self.cli.pipeline() as p:
                try:
                    p.watch('get_not_crawled_urls')
                    md5hash_list = p.zrange(self.url_rank, 0, 0)
                    if len(md5hash_list) == 0:
                        break
                    md5hash = md5hash_list[0]
                    key = self.prefix + md5hash
                    html = Html(json_str=p.get(key))
                    p.multi()
                    p.delete(key)
                    p.zrem(self.url_rank, md5hash)
                    p.execute()
                    htmls.append(html)
                except redis.WatchError:
                    continue
        return htmls

    def add_html_parsed(self, md5hashes):
        for md5hash in md5hashes:
            self.cli.sadd(self.html_parsed, md5hash)

    def add_url_unique(self, md5hashes):
        for md5hash in md5hashes:
            self.cli.sadd(self.url_unique, md5hash)

    def delete_url_unique(self):
        self.cli.delete(self.url_unique)

    def delete_all(self):
        for k in self.cli.keys(self.prefix + '*'):
            self.cli.delete(k)