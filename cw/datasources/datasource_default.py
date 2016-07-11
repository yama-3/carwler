
#!/usr/bin/env python
# -*- coding: utf-8 -*-


class DataAccess(object):
    def __init__(self, host=None, port=None, html_regex=None, option={}):
        pass

    def get_next_url(self):
        pass

    def insert_htmls(self, htmls):
        pass

    def get_next_html(self):
        pass

    def update_crawled_html(self, html):
        pass

    def store_links(self, src_link, dst_links=[]):
        pass

    def store_hosts(self, src_host, dst_hosts=[]):
        pass

    def delete_url_unique(self):
        pass

    def is_exists(self, md5hash):
        pass

    def set_property(self, md5hash, property_name, property_value):
        pass