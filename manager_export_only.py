
# -*- coding: utf-8 -*-

import sys
import threading
import time
import os
import json
import config
from datetime import datetime
import cw.datasources.datasource_redis
import cw.datasources.datasource_mongodb
from cw.datasources.cache_controller import CacheController
from cw.models.html import Html
import logger
import importlib
import file_hash_creator


if __name__ == '__main__':
    request_file = sys.argv[1]
    request = json.load(open(request_file))
    print request

    request_id = sys.argv[2]

    start_at = datetime.now()

    logger = logger.create_logger(request['logger']['level'], request['logger']['handler_type'], request['logger']['option'])

    exporter_module = importlib.import_module(request['exporter']['name'])

    mongo = cw.datasources.datasource_mongodb.DataAccess(host=config.mongo['host'],
                                                         port=config.mongo['port'],
                                                         db_name=config.mongo['db_name'],
                                                         html_regex=request['datasource']['content_type_regex_to_datasource'],
                                                         prefix=request_id)
    exporter = exporter_module.Exporter(datasource=mongo, option=request['exporter']['option'], logger=logger)

    data_count = exporter.export()
    logger.info({'message': 'export %d data' % data_count})
    logger.info({'message': 'bye'})
