
# -*- coding: utf-8 -*-

import unittest
import cw.datasources.datasource_default


class DataAccessTest(unittest.TestCase):
    def setUp(self):
        self.data_access = cw.datasources.datasource_default.DataAccess()

    def test_is_exist_get_next_url(self):
        self.data_access.get_next_url()

    def test_is_exist_insert_htmls(self):
        self.data_access.insert_htmls([])

    def test_is_exist_get_next_html(self):
        self.data_access.get_next_html()

    def test_is_exist_update_crawled_html(self):
        self.data_access.update_crawled_html(None)

    def test_is_exist_store_links(self):
        self.data_access.store_links(None, dst_links=None)

    def test_is_exist_store_hosts(self):
        self.data_access.store_hosts(None, dst_hosts=None)
