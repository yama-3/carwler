
# -*- coding: utf-8 -*-

import unittest
from cw.parsers.parser_default import Parser, Prioritizer
from cw.models.html import Html
import cw.datasources.datasource_default


class TestDataAccess(cw.datasources.datasource_default.DataAccess):
    def store_urls(self, html):
        pass


class Test(unittest.TestCase):
    def setUp(self):
        self.prioritizer = Prioritizer()
        self.parser = Parser(1, prioritizer=self.prioritizer, exclude_url_regex='.*(www\.crosswarp\.com/|www\.modd\.com/).*', datasource=TestDataAccess())

    def test_get_priority1(self):
        self.assertEqual(10, self.prioritizer.get_priority('http://www.yahoo.co.jp/weather', 10, 'http://www.yahoo.co.jp/finance'))
        self.assertEqual(10, self.prioritizer.get_priority('http://www.yahoo.co.jp/weather', 10, 'http://www.yahoo.co.jp/finance/'))

    def test_get_priority2(self):
        self.assertEqual(5, self.prioritizer.get_priority('http://www.yahoo.co.jp/weather', 10, 'http://www.google.co.jp'))
        self.assertEqual(5, self.prioritizer.get_priority('http://www.yahoo.co.jp/weather', 10, 'http://www.google.co.jp/'))

    def test_get_priority3(self):
        self.assertEqual(2, self.prioritizer.get_priority('http://www.yahoo.co.jp/weather', 5, 'http://www.google.co.jp'))

    def test_get_priority4(self):
        self.assertEqual(1, self.prioritizer.get_priority('http://www.yahoo.co.jp/weather', 2, 'http://www.google.co.jp'))
        self.assertEqual(1, self.prioritizer.get_priority('http://www.yahoo.co.jp/weather', 1, 'http://www.google.co.jp'))

    def test_parser_get_links(self):
        html = '<html><body><a href="http://www.modd.com/">modd1</a><a href="http://www.modd.com">modd2</a><a href="/info.html">information</a></body></html>'
        links = self.parser.get_links(Html(html=html))
        self.assertEqual(1, len(links))
        self.assertEqual('http://www.modd.com', links[0])

    def test_parser_get_links_return_empty_list(self):
        links = self.parser.get_links('hoge')
        self.assertEqual(0, len(links))
