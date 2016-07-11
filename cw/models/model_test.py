
# -*- coding:utf-8 -*-

import unittest
from cw.models.model import Model


class TestFunctions(unittest.TestCase):
    def test_model_to_json(self):
        m = Model()
        m.id = 1
        self.assertEqual(m.to_json(), '{"id": 1}')

    def test_model_from_json(self):
        m = Model(json_str='{"id":1, "name":"yamabe"}')
        self.assertEqual(m.id, 1)
        self.assertEqual(m.name, 'yamabe')

if __name__ == '__main__':
    unittest.main()
