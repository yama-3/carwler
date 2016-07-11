
# -*- encoding: utf-8 -*-

import unittest
import cw.crawlers.crawler_default


class CrawlerTest(unittest.TestCase):
    def setUp(self):
        self.crawler = cw.crawlers.crawler_default.Crawler(1, debug_level=1)

    def test_hoge(self):
        pass
