# -*- coding: utf-8 -*-

import sys
import threading
import time
import os
import json
from datetime import datetime
from cw.datasources.cache_controller import CacheController
from cw.models.html import Html
import logger
import importlib
import file_hash_creator
import argparse
import codecs


def build_argparser():
    argparser = argparse.ArgumentParser(description='wf.crawler manager.py')
    argparser.add_argument('--request_id', help='request id for crawling')
    argparser.add_argument('--request_file', help='configuration file for crawling')
    argparser.add_argument('--keyword_file', help='keyword file for search by crawler')
    # argparser.add_argument('--start_url', help='start url for crawling')
    argparser.add_argument('--create_file_hash', default=False, action='store_true', help='create file hash')
    return argparser


def build_datasource_cache(request_datasource, request_id, logger):
    datasource_cache_module = importlib.import_module(request_datasource['cache'])
    return datasource_cache_module.DataAccess(host=request_datasource['cache_options']['host'],
                                              port=request_datasource['cache_options']['port'],
                                              html_regex=request_datasource['content_type_regex_to_datasource'],
                                              prefix=request_id,
                                              logger=logger)


def build_datasource_backend(request_datasource, request_id, logger):
    datasource_backend_module = importlib.import_module(request_datasource['backend'])
    return datasource_backend_module.DataAccess(
        html_regex=request_datasource['content_type_regex_to_datasource'],
        prefix=request_id,
        option=request_datasource['backend_options'],
        logger=logger)


def build_exporter(request_exporter, datasource_backend, option, logger):
    exporter_module = importlib.import_module(request_exporter['name'])
    exporter_options = request_exporter['option']
    for k, v in option.items():
        exporter_options[k] = v
    return exporter_module.Exporter(datasource=datasource_backend, option=exporter_options, logger=logger)


def build_crawler_threads(request, request_id, datasource, contenttypes_store_to_datasource, thread_list, lock, logger):
    crawler_module = importlib.import_module(request['name'])
    for i in range(request['thread']):
        crawler_thread = crawler_module.Crawler(crawler_id=i,
                                                datasource=datasource,
                                                filestore_path=os.path.join(request['file_store'], request_id),
                                                default_encoding=request['default_encoding'],
                                                timeout=request['timeout'],
                                                contenttypes_store_to_datasource=contenttypes_store_to_datasource,
                                                lock=lock,
                                                cookie_file='cookie_' + request_id + '.txt',
                                                logger=logger)
        thread_list.append(crawler_thread)
        crawler_thread.start()


def build_parser_threads(request, keyword_file, datasource, thread_list, lock, logger):
    parser_module = importlib.import_module(request['name'])
    parser_options = request['options']
    if keyword_file is not None:
        parser_options['keyword_file'] = keyword_file
    for i in range(request['thread']):
        parser_thread = parser_module.Parser(parser_id=i,
                                             datasource=datasource,
                                             exclude_url_regex=request['exclude_url_regex'],
                                             options=parser_options,
                                             lock=lock,
                                             logger=logger)
        thread_list.append(parser_thread)
        parser_thread.start()


def build_cache_controller_thread(datasource_cache, datasource_backend, request, logger, exclude_thread_list):
    thread = CacheController(datasource_cache, datasource_backend, sync_count=request['sync_count'],
                                              sync_interval=request['sync_interval'], logger=logger)
    exclude_thread_list.append(thread)
    thread.start()
    return thread


if __name__ == '__main__':
    parser = build_argparser()
    args = parser.parse_args()

    if args.request_id is None or args.request_file is None:
        parser.print_help()
        sys.exit()

    request_file = os.path.abspath(args.request_file)
    if request_file is None and os.path.exists(request_file):
        parser.print_help()
        sys.exit()

    keyword_file = os.path.abspath(args.keyword_file) if args.keyword_file is not None else None
    if keyword_file is not None and not os.path.exists(keyword_file):
        parser.print_help()
        sys.exit()

    # TODO: Requestファイルを分割したい。
    # 複数のJSONをロードして、差分だけの記述にできないかな？
    request = json.load(open(request_file))

    start_at = datetime.now()
    logger = logger.create_logger(request['logger']['level'], request['logger']['handler_type'],
                                  request['logger']['option'])

    datasource_cache = build_datasource_cache(request['datasource'], args.request_id, logger) if 'cache' in request['datasource'] else None
    datasource_backend = build_datasource_backend(request['datasource'], args.request_id, logger)
    exporter_option = {'keyword_id': json.loads(codecs.open(keyword_file, 'r', 'utf16').read())['KeywordID']} if keyword_file is not None else {}
    exporter = build_exporter(request['exporter'], datasource_backend, exporter_option, logger) if 'exporter' in request else None

    # backend -> cache
    if datasource_cache is not None:
        datasource_cache.delete_url_unique()

        htmls = datasource_backend.get_htmls()
        # logger.info({'number of backend htmls': len(htmls), 'request_id': args.request_id})
        datasource_cache.add_url_unique([html.md5hash for html in htmls])

        parsed_htmls = [html.md5hash for html in htmls if html.crawled_at is not None and html.parsed_at is not None]
        logger.info({'number of parsed htmls': len(parsed_htmls), 'request_id': args.request_id})
        datasource_cache.add_html_parsed(parsed_htmls)

        crawled_htmls = [html for html in htmls if html.crawled_at is not None and html.parsed_at is None]
        logger.info({'number of crawled htmls': len(crawled_htmls), 'request_id': args.request_id})
        for html in crawled_htmls:
            datasource_cache.update_crawled_html(html)

        not_crawled_htmls = [html for html in htmls if html.crawled_at is None]
        logger.info({'number of not crawled htmls': len(not_crawled_htmls), 'request_id': args.request_id})
        for not_crawled_html in not_crawled_htmls:
            logger.debug({'not_crawled_html.url': not_crawled_html.url, 'message': not_crawled_html.to_line_string(), 'request_id': args.request_id})
        datasource_cache.insert_htmls(not_crawled_htmls)

    # 起点
    start_url = keyword_file if keyword_file is not None else request['crawler']['start_url']
    logger.debug({'start_url': start_url, 'request_id': args.request_id})
    if datasource_cache is not None:
        datasource_cache.insert_htmls([Html(url=start_url, priority=999, detect_at=datetime.now())])
    else:
        datasource_backend.insert_htmls([Html(url=start_url, priority=999, detect_at=datetime.now())])

    # 除外
    if datasource_cache is not None:
        exclude_url_list = request['manager']['exclude_url_list']
        if os.path.isfile(exclude_url_list):
            with open(exclude_url_list) as exclude_list:
                md5hashes = [line.strip() for line in exclude_list]
                datasource_cache.add_url_unique(md5hashes)
            logger.info({'number of cralwed urls(exclude_url_list)': len(md5hashes), 'request_id': args.request_id})

    # exit condition
    if 'priority_url_count' in request['manager']['exit_conditions']:
        exit_condition_url_priority = request['manager']['exit_conditions']['priority_url_count']['priority']
        exit_condition_url_count = request['manager']['exit_conditions']['priority_url_count']['count'] \
                                   + datasource_backend.get_url_count_by_priority(exit_condition_url_priority)
        logger.info({'exit_condition_url_count': exit_condition_url_count,
                     'exit_condition_url_priority': exit_condition_url_priority, 'request_id': args.request_id})
    exit_condition_end_url_md5hash = None
    if 'end_url' in request['manager']['exit_conditions']:
        exit_condition_end_url = request['manager']['exit_conditions']['end_url']['url']
        exit_condition_end_url_md5hash = Html(url=request['manager']['exit_conditions']['end_url']['url']).md5hash
        logger.info({'exit_condition_end_url': exit_condition_end_url, 'exit_condition_end_url_md5hash': exit_condition_end_url_md5hash, 'request_id': args.request_id})
    if 'crawling_period' in request['manager']['exit_conditions']:
        exit_condition_crawling_period_minutes = request['manager']['exit_conditions']['crawling_period']['minutes']
        logger.info({'exit_condition_crawling_period_minutes': exit_condition_crawling_period_minutes, 'request_id': args.request_id})
    if 'crawling_timelimit' in request['manager']['exit_conditions']:
        exit_condition_crawling_timelimit = datetime.strptime(
            request['manager']['exit_conditions']['crawling_timelimit']['datetime'], '%Y/%m/%d %H:%M')
        logger.info({'exit_condition_crawling_timelimit': exit_condition_crawling_timelimit.strftime('%Y/%m/%d %H:%M'), 'request_id': args.request_id})

    # crawler threads
    threads = []
    lock_crawler = threading.Lock()
    build_crawler_threads(request=request['crawler'],
                          request_id=args.request_id,
                          datasource=datasource_cache if datasource_cache is not None else datasource_backend,
                          contenttypes_store_to_datasource=request['datasource']['content_type_regex_to_datasource'],
                          lock=lock_crawler,
                          logger=logger,
                          thread_list=threads)

    # parser threads
    lock_parser = threading.Lock()
    build_parser_threads(request=request['parser'],
                         keyword_file=keyword_file,
                         datasource=datasource_cache if datasource_cache is not None else datasource_backend,
                         lock=lock_parser,
                         logger=logger,
                         thread_list=threads)

    exclude_threads = []
    cache_controller_thread = None
    if datasource_cache is not None:
        cache_controller_thread = build_cache_controller_thread(datasource_cache=datasource_cache,
                                                                datasource_backend=datasource_backend,
                                                                request=request['cache_controller'],
                                                                logger=logger,
                                                                exclude_thread_list=exclude_threads)

    if args.create_file_hash:
        file_hash_creator = file_hash_creator.FileHashCreator(datasource=datasource_backend, logger=logger)
        threads.append(file_hash_creator)
        file_hash_creator.start()

    main_thread = threading.currentThread()
    exclude_threads.append(main_thread)

    exit_now = False
    prev_threads_count = len(threads)
    while len(threads) > 0:
        try:
            time.sleep(1.0)
            threads = [t for t in threading.enumerate() if t not in exclude_threads]
            if not prev_threads_count == len(threads):
                logger.error({'message': 'maybe crawler thread was crashed.', 'from': prev_threads_count, 'to': len(threads), 'request_id': args.request_id})
                prev_threads_count = len(threads)

            if exit_now:
                continue

            #if datasource_backend.get_url_count_by_priority(exit_condition_url_priority) >= exit_condition_url_count:
            #    logger.info({'exit_condition': 'url_count', 'request_id': args.request_id})
            #    for t in threads:
            #        t.kill_received = True
            #    exit_now = True
            if exit_condition_end_url_md5hash is not None and datasource_backend.is_exists(exit_condition_end_url_md5hash):
                logger.info({'exit_condition': 'end_url', 'request_id': args.request_id})
                for t in threads:
                    t.kill_received = True
                exit_now = True
            if int((datetime.now() - start_at).total_seconds()) / 60 >= exit_condition_crawling_period_minutes:
                logger.info({'exit_condition': 'period', 'request_id': args.request_id})
                for t in threads:
                    t.kill_received = True
                exit_now = True
            if exit_condition_crawling_timelimit <= datetime.now():
                logger.info({'exit_condition': 'timelimit', 'request_id': args.request_id})
                for t in threads:
                    t.kill_received = True
                exit_now = True
        except (KeyboardInterrupt, SystemExit):
            logger.info({'message': 'Received keybord interrupt, quitting threads', 'request_id': args.request_id})
            for t in threads:
                t.kill_received = True
            exit_now = True

    if cache_controller_thread is not None:
        threads.append(cache_controller_thread)
        exclude_threads.remove(cache_controller_thread)
        cache_controller_thread.kill_received = True

    while len(threads) > 0:
        threads = [t for t in threading.enumerate() if t not in exclude_threads]
        time.sleep(1.0)
    logger.info({'message': 'all exit', 'request_id': args.request_id})

    if exporter is not None:
        data_count = exporter.export()
        logger.info({'message': 'export %d data' % (data_count), 'request_id': args.request_id})

    logger.info({'message': 'bye', 'request_id': args.request_id})
