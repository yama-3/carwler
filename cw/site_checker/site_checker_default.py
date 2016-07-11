
#!/opt/python/bin/python
# -*- encoding: utf-8 -*-

import BeautifulSoup


class SiteChecker:
    def __init__(self):
        self.indent = 0
        self.result = ''
        self.IGNORE_TAGS = ['script']
        self.indent_string = '  '
        self.root = None

    def get_soup(self, html):
        return BeautifulSoup.BeautifulSoup(html)

    def parse(self, html):
        soup = self.get_soup(html)
        self.root = Tag(soup.body.name)
        self._parse(self.root, soup.body)

    def _parse(self, tag, obj):
        for c in obj.contents:
            if c.__class__ == BeautifulSoup.Tag and c.name not in self.IGNORE_TAGS:
                child = Tag(c.name, c['id'] if c.has_key('id') else None, c['class'] if c.has_key('class') else None)
                tag.add_child(child)
                self._parse(child, c)


class Tag:
    def __init__(self, name, id='', klass='', indent=0, indent_str='  '):
        self.name = name
        self.id = id if id is not None and len(id) > 0 else None
        self.klass = klass if klass is not None and len(klass) > 0 else None
        self.children = []
        self.indent = 0
        self.indent_str = indent_str

    def __str__(self):
        return '{0}{1},{2},{3}'.format(self.indent_str * self.indent, self.name, self.id if self.id is not None else '', self.klass if self.klass is not None else '')

    def add_child(self, child):
        if child.__class__ != Tag:
            return
        if len([c for c in self.children if c.name == child.name and c.id == child.id and c.klass == child.klass]) > 0:
            return
        child.indent = self.indent + 1
        child.indent_str = self.indent_str
        self.children.append(child)

    def dump(self):
        s = str(self) + '\n'
        for child in self.children:
            s += child.dump()
        return s
