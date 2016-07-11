
#!/opt/python27/bin/python
# -*- coding: utf-8 -*-

import threading
import time
import hashlib
import os


class FileHashCreator(threading.Thread):
    def __init__(self, datasource=None, logger=None):
        threading.Thread.__init__(self)
        self.datasource = datasource
        self.logger = logger
        self.kill_received = False

    def run(self):
        counter = 0
        threshold = 60 * 3
        while not self.kill_received:
            time.sleep(1.0)
            html = self.datasource.htmls.find_one({'file_path': {'$exists': True}, 'file_hash': {'$exists': False}})
            if html is None:
                counter += 1
                if counter > threshold:
                    break
                continue
            counter = 0
            if not os.path.exists(html['file_path']):
                self.logger.info('file_path not exists: %s' % html['url'])
                continue
            file_hash = self.get_file_hash(html['file_path'])
            self.datasource.set_property(html['md5hash'], 'file_hash', file_hash)
        self.logger.info('FileHashCreator exit')

    def get_file_hash(self, file_path):
        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(1024*1024), b''):
                md5.update(chunk)
        return md5.hexdigest()
