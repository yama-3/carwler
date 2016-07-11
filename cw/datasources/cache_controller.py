
# -*- coding:utf-8 -*-

import threading
import time

LOOP_THRESHOLD = 1800
SYNC_INTERVAL = 30
SYNC_COUNT = 100


class CacheController(threading.Thread):
    def __init__(self,
                 redis_datasource,
                 mongodb_datasource,
                 sync_count=SYNC_COUNT,
                 sync_interval=SYNC_INTERVAL,
                 loop_threshold=LOOP_THRESHOLD,
                 logger=None):
        threading.Thread.__init__(self)
        self.redis = redis_datasource
        self.mongodb = mongodb_datasource
        self.sync_count = sync_count
        self.sync_interval = sync_interval
        self.loop_threshold = loop_threshold
        self.kill_received = False
        self.logger = logger

    def get_html_parsed_count(self):
        return self.redis.cli.scard(self.redis.html_parsed)

    def is_sync_redis2mongo(self):
        html_parsed_count = self.get_html_parsed_count()
        if html_parsed_count > 0:
            return True
        return False

    def get_url_rank_count(self):
        return self.redis.cli.zcard(self.redis.url_rank)

    def is_sync_mongo2redis(self):
        return False

    def redis2mongo(self):
        htmls = self.redis.get_parsed_htmls(self.sync_count)
        html_count = len(htmls)
        if html_count > 0:
            exist_htmls, not_exist_htmls = [], []
            for html in htmls:
                if self.mongodb.is_exists(html.md5hash):
                    exist_htmls.append(html)
                else:
                    not_exist_htmls.append(html)
            if len(not_exist_htmls) > 0:
                self.mongodb.insert_htmls(not_exist_htmls)
            if len(exist_htmls) > 0:
                for html in exist_htmls:
                    self.mongodb.update_crawled_html(html)
                    self.mongodb.update_parsed_html(html)
        return html_count

    def cleanup(self):
        while self.is_sync_redis2mongo():
            html_count = self.redis2mongo()
            self.logger.debug({'cache_controller_id': 0, 'message': '%s -> %s (parsed) %d / %d' % (self.redis.__class__, self.mongodb.__class__, html_count, self.sync_count)})

        htmls = self.redis.get_not_parsed_htmls()
        if len(htmls) > 0:
            self.mongodb.insert_htmls(htmls)
            self.logger.debug({'cache_controller_id': 0, 'message': '%s -> %s (not parsed) %d' % (self.redis.__class__, self.mongodb.__class__, len(htmls))})

        htmls = self.redis.get_not_crawled_urls()
        if len(htmls) > 0:
            self.mongodb.insert_htmls(htmls)
            self.logger.debug({'cache_controller_id': 0, 'message': '%s -> %s (not crawled) %d' % (self.redis.__class__, self.mongodb.__class__, len(htmls))})

        self.redis.delete_all()

    def run(self):
        counter = 0
        try:
            while not self.kill_received:
                time.sleep(1.0)
                counter += 1

                if counter < self.sync_interval:
                    continue

                if counter > self.loop_threshold:
                    break

                if self.is_sync_redis2mongo():
                    html_count = self.redis2mongo()
                    self.logger.debug({'cache_controller_id': 0, 'message': '%s -> %s %d / %d' % (self.redis.__class__, self.mongodb.__class__, html_count, self.sync_count)})
                    counter = 0
                    continue

                if self.is_sync_mongo2redis():
                    self.logger.debug({'cache_controller_id': 0, 'message': '%s -> %s' % (self.mongodb.__class__, self.redis.__class__)})
                    counter = 0
                    continue
        except:
            self.logger.exception({'message': 'unknown exception'})

        self.cleanup()

        self.logger.info({'cache_controller_id': 0, 'message': 'exit'})
