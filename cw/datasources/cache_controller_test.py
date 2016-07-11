
# -*- coding:utf-8 -*-

import unittest
import mock
from cw.models.html import Html
from cw.datasources.cache_controller import CacheController


class Test(unittest.TestCase):
    def setUp(self):
        self.redis = mock.MagicMock()
        self.mongo = mock.MagicMock()
        self.cache_controller = CacheController(self.redis, self.mongo, loop_threshold=5, sync_interval=1, logger=mock.MagicMock())

    def test_is_sync_redis2mongo_1000(self):
        self.redis.cli.scard = mock.Mock()
        self.redis.cli.scard.return_value = 1000
        self.assertEqual(True, self.cache_controller.is_sync_redis2mongo())

    def test_is_sync_redis2mongo_0(self):
        self.redis.cli.scard = mock.Mock()
        self.redis.cli.scard.return_value = 0
        self.assertEqual(False, self.cache_controller.is_sync_redis2mongo())

    def test_is_sync_mongo2redis(self):
        self.assertEqual(False, self.cache_controller.is_sync_mongo2redis())

    def test_run_all(self):
        self.redis.cli.scard = mock.Mock(side_effect=[1000, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        self.redis.cli.zcard.return_value = mock.Mock(side_effect=[1, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        htmls = [Html(url='http://www.crosswarp.com/'),
                 Html(url='http://cs.crosswarp.com/'),
                 Html(url='http://cr.crosswarp.com/')]
        self.redis.get_parsed_htmls.return_value = htmls

        self.cache_controller.run()

        self.assertTrue(self.redis.get_parsed_htmls.call_count > 0)
        self.assertTrue(self.mongo.insert_htmls.call_count > 0)

    def test_run_no_parsed_htmls(self):
        self.redis.cli.scard.return_value = 0
        self.redis.cli.zcard.return_value = 0
        self.redis.get_parsed_htmls.return_value = []

        self.cache_controller.run()

        self.assertTrue(self.redis.get_parsed_htmls.call_count == 0)
        self.assertTrue(self.mongo.update_crawled_html.call_count == 0)
