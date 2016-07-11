
# -*- coding:utf-8 -*-

import hashlib
from cw.models.model import Model


class Html(Model):
    def __init__(self,
                 url='',
                 response_url='',
                 priority=1,
                 cookie='',
                 referer='',
                 html='',
                 request_headers={},
                 response_headers={},
                 response_code=0,
                 content_type='',
                 properties={},
                 destinations=[],
                 detect_at=None,
                 crawled_at=None,
                 parsed_at=None,
                 json_str=None):
        if json_str is None:
            self.url = url
            self.response_url = response_url
            self.priority = priority
            self.cookie = cookie
            self.referer = referer
            self.html = html
            self.request_headers = request_headers
            self.response_headers = response_headers
            self.response_code = response_code
            self.content_type = content_type
            self.properties = properties
            self.destinations = []
            self.detect_at = detect_at
            self.crawled_at = crawled_at
            self.parsed_at = parsed_at
        else:
            super(Html, self).from_json(json_str)
        self.md5hash = self.generate_md5hash()

    def generate_md5hash(self):
        return hashlib.md5(self.url.lower()).hexdigest()

    def to_line_string(self):
        return '{0}\t{1}\t\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}'.format(
            self.url,
            self.response_url if 'response_url' in self.__dict__ else '',
            self.referer if 'referer' in self.__dict__ else '',
            self.priority,
            self.response_code,
            self.content_type if 'content_type' in self.__dict__ else '',
            self.detect_at.strftime('%Y/%m/%d %H:%M:%S') if 'detect_at' in self.__dict__ and self.detect_at is not None else '',
            self.crawled_at.strftime('%Y/%m/%d %H:%M:%S') if 'crawled_at' in self.__dict__ and self.crawled_at is not None else '',
            self.parsed_at.strftime('%Y/%m/%d %H:%M:%S') if 'parsed_at' in self.__dict__ and self.parsed_at is not None else '',
            self.file_path if 'file_path' in self.__dict__ else '')

    def from_dict(self, dic):
        if not isinstance(dic, dict):
            return
        self.__dict__ = dic
        return self
