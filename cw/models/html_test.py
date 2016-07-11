
# -*- coding:utf-8 -*-

import unittest
from cw.models.html import Html


class TestFunctions(unittest.TestCase):
    def test_html_to_json(self):
        #m = Html(url='http://www.crosswarp.com/info/', priority=10, cookie='hoge', referer='http://www.crosswarp.com/')
        m = Html()
        print m.to_json()
        self.assertEqual(m.to_json(), '{"priority": 1, "response_code": 0, "md5hash": "d41d8cd98f00b204e9800998ecf8427e"}')

    def test_html_from_json(self):
        m = Html(json_str='{"url": "http://www.crosswarp.com", "priority": 10}')
        self.assertEqual(m.priority, 10)
        self.assertEqual(m.url, 'http://www.crosswarp.com')
