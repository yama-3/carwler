
#!/opt/python27/bin/python
# -*- coding: utf-8 -*-

import urllib2
import gzip
import StringIO
import threading
import time
import os
import urllib
import urlparse
import re
import cookielib
from datetime import datetime
from cw.models.html import Html


loop_threshold = 900
encoding_dict = {
    'gbk': 'cp936',
    'gb2312': 'cp936'
}


class Crawler(threading.Thread):
    def __init__(self,
                 crawler_id,
                 datasource=None,
                 contenttypes_store_to_datasource=r'text/.*',
                 filestore_path='.',
                 timeout=30,
                 default_encoding='utf-8',
                 debug_level=0,
                 lock=None,
                 request_headers={},
                 cookie_file=None,
                 logger=None):
        threading.Thread.__init__(self)
        self.crawler_id = crawler_id
        self.datasource = datasource
        self.kill_received = False
        self.contenttypes_store_to_datasource_re = re.compile(contenttypes_store_to_datasource, re.IGNORECASE)
        self.filestore_path = filestore_path
        if not os.path.exists(self.filestore_path):
            os.makedirs(self.filestore_path)
        self.timeout = timeout
        self.default_encoding = default_encoding
        self.debug_level = debug_level
        self.lock = lock
        self.request_headers = {
            'Accept': '*/*',
            'Accept-Language': 'ja-JP',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.106 Safari/535.2'}
        for header in request_headers:
            self.request_headers[header] = request_headers[header]
        self.cookie_file = cookie_file
        self.logger = logger

    def quote_url(self, url):
        if '%' in url or url.startswith('/'):
            return url
        up = urlparse.urlparse(url.encode('utf-8'))
        s, n, p, q = up.scheme, up.netloc, up.path, '?' + up.query if len(up.query) else ''
        return s + '://' + n + urllib.quote(p) + q

    def get_html(self, url, referer=None):
        if referer is not None:
            self.request_headers['Referer'] = referer
        html_bytes, response_headers, request_headers, response_code, response_url, encoding = None, None, None, None, None, None
        response = None
        timeout = self.timeout
        while True:
            try:
                request = urllib2.Request(url, headers=self.request_headers)
                cookie_jar = cookielib.LWPCookieJar()
                if self.cookie_file is not None and os.path.isfile(self.cookie_file):
                    with self.lock:
                        cookie_jar.load(self.cookie_file)
                opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie_jar), urllib2.HTTPHandler(debuglevel=self.debug_level))
                response = opener.open(request, timeout=timeout)
                if self.cookie_file is not None:
                    with self.lock:
                        cookie_jar.save(self.cookie_file, ignore_discard=True, ignore_expires=True)
                if response is None:
                    return html_bytes, response_headers, request_headers, response_code, response_url, encoding
                if 'content-encoding' in response.headers and response.headers['content-encoding'] == 'gzip':
                    html_bytes = self.read_compressed(response.read())
                else:
                    html_bytes = response.read()
                encoding = self.detect_encoding(response.headers['Content-Type'] if 'Content-Type' in response.headers else None, html_bytes[:1000])
                request_headers = request.headers
                response_headers, response_code, response_url = response.headers.dict, response.code, response.url
            except urllib2.HTTPError, e:
                self.logger.exception({'crawler_id': self.crawler_id, 'url': url, 'status_code': e.code})
                html_bytes = None
                if response is not None:
                    response_headers, response_url = response.header, response.url
                request_headers = request.headers
                response_code = e.code
            except urllib2.URLError, e:
                self.logger.exception({'crawler_id': self.crawler_id, 'url': url})
                timeout *= 2
                if timeout < 300:
                    continue
            except Exception, e:
                self.logger.exception({'crawler_id': self.crawler_id, 'url': url})
            return html_bytes, response_headers, request_headers, response_code, response_url, encoding

    def detect_encoding(self, content_type, bytes):
        encoding = self.default_encoding
        s = re.compile(r'.*charset=(?P<charset>[^\"\']+)', re.IGNORECASE).search(content_type)
        if s is not None and 'charset' in s.groupdict():
            encoding = s.groupdict()['charset']
        s = re.compile(r'<meta.*?charset=[\"\']{0,1}(?P<charset>[^\"\']+)[\"\']{0,1}', re.IGNORECASE).search(bytes)
        if s is not None and 'charset' in s.groupdict():
            encoding = s.groupdict()['charset'].split(';')[0]
        #self.logger.debug({'crawler_id': self.crawler_id, 'encoding': encoding})
        if encoding in encoding_dict:
            encoding = encoding_dict[encoding]
        return encoding

    def run(self):
        self.logger.info({'message': 'start crawler', 'crawler_id': self.crawler_id})
        loop = 0
        while not self.kill_received:
            url_quoted_text = ''
            try:
                time.sleep(1.0)
                with self.lock:
                    html = self.datasource.get_next_url()
                if html is None:
                    if loop > loop_threshold:
                        break
                    loop += 1
                    continue
                loop = 0
                priority = html.priority
                if priority < 0:
                    continue
                url_text = html.url
                url_quoted_text = self.quote_url(url_text)
                self.logger.debug({'crawler_id': self.crawler_id, 'url': html.url, 'quoted_url': url_quoted_text})
                referer = html.referer if 'referer' in dir(html) else None
                html_bytes, response_headers, request_headers, response_code, response_url, html_encoding = self.get_html(url_quoted_text, referer)
                # if response_code == 0:
                #     self.datasource.insert_htmls([Html(url=html.url, priority=html.priority-1, referer=html.referer, detect_at=html.detect_at)])
                #     continue
                html.crawled_at = datetime.now()
                content_type = response_headers['content-type'] if response_headers is not None and 'content-type' in response_headers else ''
                html.response_headers, html.request_headers, html.response_code, html.response_url, html.content_type = response_headers, request_headers, response_code, response_url, content_type
                if html_bytes is not None and self.contenttypes_store_to_datasource_re.match(content_type):
                    html.html = html_bytes.decode(html_encoding, 'ignore')

                self.logger.debug({'crawler_id': self.crawler_id,
                                   'url': html.url,
                                   'priority': html.priority,
                                   'content_type': html.content_type})

                # content-type が指定されたものにマッチするか、レスポンスコードが400以上
                if self.contenttypes_store_to_datasource_re.match(html.content_type) or html.response_code >= 400:
                    if html.response_code >= 400:
                        html.parsed_at = datetime.now()
                else:
                    if html_bytes is not None and len(html_bytes) > 0:
                        file_path = self.generate_filepath(content_type, url_quoted_text, str(html.md5hash))
                        html.file_path = file_path
                        html.parsed_at = datetime.now()
                        try:
                            with open(file_path, 'wb') as f:
                                f.write(html_bytes)
                        except:
                            self.logger.exception({'craler_id': self.crawler_id, 'url': html.url})
                    else:
                        html.parsed_at = datetime.now()

                #if 'html' not in dir(html) and 'file_path' not in dir(html):
                #    self.logger.info('probably timeout: %s' % url_quoted_text)
                #    continue

                with self.lock:
                    self.datasource.update_crawled_html(html)
            except:
                self.logger.exception('unknown error (crawler_id: %s, url: %s)' % (str(self.crawler_id), url_quoted_text))
        self.logger.info({'crawler_id': self.crawler_id, 'message': 'exit'})

    def generate_filepath(self, content_type, url_quoted_text, oid):
        ext = '.' + content_type.split('/')[1] if content_type.find('image') >= 0 or content_type.find('video') >= 0 or content_type.find('application') >= 0 else ''
        dir_path = self.filestore_path + os.sep + datetime.now().strftime('%Y%m%d' + os.sep + '%H')
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path)
            except os.error, e:
                self.logger.exception({'exception_message': e.message})
        return dir_path + os.sep + oid + ext

    def read_compressed(self, compressed_data):
        compressed_stream = StringIO.StringIO(compressed_data)
        gzipper = gzip.GzipFile(fileobj=compressed_stream)
        return gzipper.read()
