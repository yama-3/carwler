
# -*- coding:utf-8 -*-

import unittest
import cw.datasources.datasource_redis
from cw.models.html import Html
import redis
from datetime import datetime


class TestFunctions(unittest.TestCase):
    def setUp(self):
        print 'setUp'
        self.cli = redis.Redis()
        self.cli.delete('redis_test_url_rank')
        self.cli.delete('redis_test_url_unique')
        self.cli.delete('redis_test_html_rank')
        self.cli.delete('redis_test_html_parsed')
        for key in self.cli.keys('redis_test_*'):
            self.cli.delete(key)
        self.data_access = cw.datasources.datasource_redis.DataAccess(prefix='redis_test')

    def tearDown(self):
        print 'tearDown'
        self.cli.delete('ds_redis_test_url_rank')
        self.cli.delete('ds_redis_test_url_unique')
        self.cli.delete('ds_redis_test_html_rank')
        self.cli.delete('redis_test_html_parsed')
        for key in self.cli.keys('redis_test_*'):
            self.cli.delete(key)

    def test_redis_url(self):
        print 'test_redis_url'
        html0 = Html(url='http://rs.crosswarp.com/', priority=1, detect_at=datetime.now())
        self.data_access.insert_htmls([html0])
        html1 = Html(url='http://www.crosswarp.com/', priority=10)
        html2 = Html(url='http://cs.crosswarp.com/', priority=100)
        self.data_access.insert_htmls([html1, html2])
        self.assertEqual(self.cli.zcard(self.data_access.url_rank), 3)
        a1 = self.data_access.get_next_url()
        self.assertEqual(a1.md5hash, html2.md5hash)
        a2 = self.data_access.get_next_url()
        self.assertEqual(a2.md5hash, html1.md5hash)

    def test_redis_url_noexists_referer(self):
        html0 = Html(url='http://rs.crosswarp.com/')
        html1 = Html(url='http://www.crosswarp.com/', priority=10)
        html0.destinations.append(html1.url)
        html1.referer = html0.url
        self.data_access.insert_htmls([html1])
        self.assertEqual(self.cli.zcard(self.data_access.url_rank), 1)

    def test_redis_html(self):
        print 'test_redis_html'
        html1 = Html(url='http://www.crosswarp.com/', priority=10)
        html2 = Html(url='http://cs.crosswarp.com/', priority=100)
        self.data_access.update_crawled_html(html1)
        self.data_access.update_crawled_html(html2)
        self.assertEqual(self.cli.zcard(self.data_access.html_rank), 2)
        a1 = self.data_access.get_next_html()
        self.assertEqual(a1.md5hash, html2.md5hash)
        a2 = self.data_access.get_next_html()
        self.assertEqual(a2.md5hash, html1.md5hash)

    def test_get_parsed_htmls(self):
        html1 = Html(url='http://www.crosswarp.com/', priority=10, parsed_at=datetime.now(), response_code=200, detect_at=datetime.now(), crawled_at=datetime.now())
        html2 = Html(url='http://cs.crosswarp.com/', priority=100, parsed_at=datetime.now(), response_code=200, detect_at=datetime.now(), crawled_at=datetime.now())
        self.data_access.update_crawled_html(html1)
        self.data_access.update_crawled_html(html2)
        self.data_access.cli.sadd(self.data_access.html_parsed, html1.md5hash)
        self.data_access.cli.sadd(self.data_access.html_parsed, html2.md5hash)

        htmls = self.data_access.get_parsed_htmls(100)
        in_loop = False
        for html in htmls:
            in_loop = True
            print html.to_line_string()
            self.assertTrue(isinstance(html, Html))
        self.assertTrue(in_loop)
