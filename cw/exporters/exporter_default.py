
# -*- coding: utf-8 -*-

import codecs


class Exporter:
    def __init__(self, datasource=None, option=None, logger=None):
        self.datasource = datasource
        self.logger = logger
        self.filename = option['filename']

    def export(self):
        with codecs.open(self.filename, 'w', 'utf-8') as f:
            htmls = self.datasource.get_htmls()
            for html in htmls:
                f.write(html.to_line_string() + '\n')
        return len(htmls)
