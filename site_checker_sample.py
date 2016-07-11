
#!/opt/python/bin/python
# -*- encoding: utf-8 -*-

import sys
from cw.crawlers.crawler_default import Crawler
from cw.site_checker.site_checker_default import SiteChecker
import codecs
import filecmp
from datetime import datetime
import os.path

output_directory = ''
demiliter = ''
temp = 'temp'
if sys.platform == 'win32':
    output_directory = r'.\work'
    demiliter = '\\'
else:
    output_directory = r'/home/yamabe/work/site_checker/Src/work'
    demiliter = '/'

if __name__ == '__main__':
    name = sys.argv[1]
    url = sys.argv[2]

    now = datetime.now()
    old_file_name = '{0}{1}{2}{3}{4}.txt'.format(output_directory, demiliter, temp, demiliter, name)
    new_file_name = '{0}{1}{2}{3}{4}.{5}.txt'.format(output_directory, demiliter, temp, demiliter, name, now.strftime('%Y%m%d.%H%M%S'))
    ng_file_name = '{0}{1}{2}.NG'.format(output_directory, demiliter, name)

    crawler = Crawler(1)
    html, response_headers, request_headers, response_code = crawler.get_html(url)
    checker = SiteChecker()
    checker.parse(html)

    result = checker.root.dump()

    f = codecs.open(new_file_name, 'w', 'utf-8')
    f.write(result)
    f.close()

    if os.path.exists(old_file_name):
        is_ok = filecmp.cmp(old_file_name, new_file_name)
        print name, 'OK' if is_ok else 'NG'
        if not is_ok:
            f = open(ng_file_name, 'w')
            f.write('')
            f.close()

    f = codecs.open(old_file_name, 'w', 'utf-8')
    f.write(result)
    f.close()
