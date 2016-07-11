
# -*- coding: utf-8 -*-

from optparse import Values
import urllib
import re
from apiclient.discovery import build
from cw.utils.constant import *


def build_options(query=''):
    options = Values()
    options.ensure_value('query', query)
    options.ensure_value('type', 'video')
    options.ensure_value('maxResults', 50)
    options.ensure_value('regionCode', 'jp')
    options.ensure_value('order', 'viewCount')
    return options

def build_freebase_url(query):
    options = build_options(query)
    freebase_params = dict(query=options.query, key=DEVELOPER_KEY)
    return FREEBASE_SEARCH_URL + '?%s' % urllib.urlencode(freebase_params)

def build_youtube():
    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)

def get_video(videoId):
    youtube = build_youtube()
    parts = 'id,snippet,contentDetails,statistics,status,topicDetails'
    list_response = youtube.videos().list(part=parts, id=videoId).execute()
    items = list_response.get('items', []) if list_response is not None else []
    return items[0] if len(items) > 0 else None

def get_video_view_count(video):
    return int(video['statistics']['viewCount'])

def get_video_playtime(video):
    sre = re.compile('PT((?P<h>\d+)H)?((?P<m>\d+)M)?(?P<s>\d+)S').search(video['contentDetails']['duration'])
    if sre is not None:
        h, m, s = sre.group('h'), sre.group('m'), sre.group('s')
        h = h if h is not None else '0'
        m = m if m is not None else '0'
        return int(h) * 3600 + int(m) * 60 + int(s)
    return -1
