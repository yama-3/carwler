
# -*- coding: utf-8 -*-

import unittest
import cw.datasources.datasource_mongodb
from cw.models.html import Html


class MongoDbTest(unittest.TestCase):
    def setUp(self):
        self.datasource = cw.datasources.datasource_mongodb.DataAccess()
        self.datasource.get_db()['_htmls'].drop()
        self.datasource.get_db()['_hosts'].drop()

    def tearDown(self):
        pass

    def test_get_next_url_and_store_htmls(self):
        self.datasource.insert_htmls([Html(url='http://www.crosswarp.com/', priority=0)])
        self.datasource.insert_htmls([Html(url='http://rs.crosswarp.com/', priority=10, referer='http://www.crosswarp.com/'),
                                      Html(url='http://cs.crosswarp.com/', priority=1)])
        self.assertEqual(3, self.datasource.get_db()['_htmls'].count())

        html = self.datasource.get_next_url()
        self.assertEqual('http://rs.crosswarp.com/', html.url)

        html = self.datasource.get_next_url()
        self.assertEqual('http://cs.crosswarp.com/', html.url)

        html = self.datasource.get_next_url()
        html = self.datasource.get_next_url()
        self.assertEqual(None, html)

    def test_get_next_html_and_update_crawled_html(self):
        html1 = Html(url='http://cs.crosswarp.com')
        self.datasource.insert_htmls([html1])
        self.assertEqual(1, self.datasource.get_db()['_htmls'].count())

        html1.html = '<html><body></body></html>'
        with self.assertRaises(TypeError):
            self.datasource.update_crawled_html(None)

        self.datasource.update_crawled_html(html1)
        html2 = self.datasource.get_next_html()
        self.assertEquals(None, html2)

        html1.content_type = 'text/html'
        self.datasource.update_crawled_html(html1)
        html2 = self.datasource.get_next_html()
        self.assertEquals(html1.html, html2.html)

    def test_store_links(self):
        html1 = Html(url='http://cs.crosswarp.com')
        self.datasource.insert_htmls([html1])
        self.assertEqual(1, self.datasource.get_db()['_htmls'].count())

        self.datasource.store_links('http://cs.crosswarp.com', ['http://rs.crosswarp.com'])
        obj = self.datasource.get_db()['_htmls'].find_one({'md5hash': html1.md5hash})
        html2 = Html()
        for key in obj:
            html2.__dict__[key] = obj[key]

        self.assertEqual(1, len(html2.destinations))
        self.assertEqual('http://rs.crosswarp.com', html2.destinations[0])

    def test_store_hosts(self):
        self.datasource.store_hosts('www.crosswarp.com', ['cs.crosswarp.com', 'rs.crosswarp.com'])
        self.assertEquals(1, self.datasource.get_db()['_hosts'].count())
