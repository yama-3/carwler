
#!/opt/python27/bin/python
# -*- coding: utf-8 -*-

import threading
import time
from BeautifulSoup import BeautifulSoup
import urlparse
import re
from datetime import datetime
from cw.models.html import Html

#loop_threshold = 900


class Prioritizer():
    def __init__(self):
        pass

    def get_priority(self, source_url, source_priority, destination_url):
        source_host = urlparse.urlparse(source_url).hostname
        destination_host = urlparse.urlparse(destination_url).hostname
        if source_host == destination_host:
            return source_priority
        return source_priority / 2 if source_priority / 2 >= 1 else 1


class Parser(threading.Thread):
    def __init__(self,
                 parser_id,
                 datasource=None,
                 prioritizer=Prioritizer(),
                 exclude_url_regex=r'http://(maps.google.com|.*\.go.jp|.*\.gov|.*\.mil)/.*',
                 loop_threshold=900,
                 option={},
                 lock=None,
                 logger=None):
        threading.Thread.__init__(self)
        self.datasource = datasource
        self.parser_id = parser_id
        self.kill_received = False
        self.prioritizer = prioritizer
        self.exclude_url_re = re.compile(str(exclude_url_regex), re.IGNORECASE)
        self.loop_threshold = loop_threshold
        self.lock = lock
        self.logger = logger

    def run(self):
        self.logger.info({'message': 'start parser', 'parser_id': self.parser_id})
        try:
            while not self.kill_received:
                time.sleep(1.0)
                with self.lock:
                    html = self.datasource.get_next_html()
                if html is None:
                    continue
                self.logger.debug({'parser_id': self.parser_id, 'url': html.url, 'priority': html.priority})
                links, referer = [], None
                try:
                    links, referer = self.get_links(html)
                except:
                    self.logger.exception({'parser_id': self.parser_id, 'message': 'error at get_links', 'url': html.url})
                html.parsed_at = datetime.now()
                html.destinations = links
                with self.lock:
                    self.datasource.update_parsed_html(html)
                dst_htmls = [Html(url=u,
                                  priority=self.prioritizer.get_priority(html.url, html.priority, u),
                                  referer=html.url if referer is None else referer,
                                  detect_at=datetime.now())
                             for u in links if u is not None and len(u) > 0 and u != html.url]
                if len(dst_htmls) > 0:
                    with self.lock:
                        self.datasource.insert_htmls(dst_htmls)
        except:
            self.logger.exception({'parser_id': self.parser_id,'message': 'unknown error'})
        self.logger.info({'parser_id': self.parser_id, 'message': 'exit'})

    def get_links(self, html, with_link_text=False):
        """
        引数で渡されたHtmlクラスのインスタンスを解析して、文字列型のURLの配列を返す
        """
        links, referer = [], None
        if isinstance(html, Html):
            soup = self.get_soup(html.html)
            if not with_link_text:
                links = list(set([u for u in [urlparse.urljoin(html.url, a['href'].strip()).strip().split('#')[0] for a in soup.findAll('a') if a.has_key('href')] if urlparse.urlparse(u).scheme in ('http', 'https') and self.exclude_url_re.match(u) is None]))
            else:
                links = list(set([u for u in [(urlparse.urljoin(html.url, a['href'].strip()).strip().split('#')[0], a.text) for a in soup.findAll('a') if a.has_key('href')] if urlparse.urlparse(u[0]).scheme in ('http', 'https') and self.exclude_url_re.match(u[0]) is None]))
        return links, referer

    def get_soup(self, html):
        return BeautifulSoup(html)
