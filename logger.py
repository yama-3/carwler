
# -*- coding: utf-8 -*-

import logging
import logging.handlers
import socket
import fluent.handler

LOG_LEVELS = {'debug': logging.DEBUG, 'info': logging.INFO, 'warning': logging.WARNING, 'error': logging.ERROR, 'critical': logging.CRITICAL}


def create_logger(log_level, handler_type, handler_option):
    print log_level, handler_type, handler_option

    logger = logging.getLogger(socket.gethostname())
    logger.setLevel(LOG_LEVELS[log_level])

    if handler_type == 'rotating_file':
        log_handler = logging.handlers.RotatingFileHandler(handler_option['filename'],
                                                           maxBytes=handler_option['max_bytes'],
                                                           backupCount=handler_option['backup_count'])
        log_handler.setFormatter(logging.Formatter(handler_option['log_format']))
    elif handler_type == 'timed_rotating_file':
        log_handler = logging.handlers.TimedRotatingFileHandler(handler_option['filename'],
                                                                when=handler_option['when'],
                                                                backupCount=handler_option['backup_count'])
        log_handler.setFormatter(logging.Formatter(handler_option['log_format']))
    elif handler_type == 'fluentd':
        log_handler = fluent.handler.FluentHandler(handler_option['tag'],
                                                   host=handler_option['host'],
                                                   port=handler_option['port'])

    log_handler.setLevel(LOG_LEVELS[log_level])
    logger.addHandler(log_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    logger.addHandler(stream_handler)

    return logger
