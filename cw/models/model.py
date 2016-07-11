
# -*- coding:utf-8 -*-

import bson.json_util


class Model(object):
    def __init__(self, json_str=None):
        if json_str is not None:
            self.from_json(json_str)

    def to_json(self):
        return bson.json_util.dumps(self.to_dict(), indent=4)

    def from_json(self, json_str):
        self.__init__()
        d = bson.json_util.loads(json_str)
        for key in d:
            self.__dict__[key] = d[key]

    def to_dict(self):
        d = {}
        for key in self.__dict__:
            if self.__dict__[key] is None:
                continue
            if '__len__' not in dir(self.__dict__[key]):
                d[key] = self.__dict__[key]
            elif len(self.__dict__[key]) > 0:
                d[key] = self.__dict__[key]
        return d

    def from_dict(self, d):
        self.__init__()
        for key in d:
            self.__dict__[key] = d[key]
