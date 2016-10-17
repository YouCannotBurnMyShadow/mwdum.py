#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from random import random
from dateutil.parser import parse
from datetime import datetime

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

uprint = lambda text: sys.stdout.buffer.write((text+'\n').encode('utf-8'))
#uprint = lambda x: "" if x else ""

wiki_namespaces = ["Talk",
    "User","User_talk","Wikipedia","Wikipedia_talk",
    "File","File_talk","MediaWiki","MediaWiki_talk",
    "Template","Template_talk","Help","Help_talk",
    "Category","Category_talk","Portal","Portal_talk",
    "Book","Book_talk",
    "Education_Program","Education_Program_talk",
    "TimedText","TimedText_talk",
    "Module","Module_talk"]

TABLE_STRUCTURE = '''
DROP TABLE IF EXISTS `page`;
CREATE TABLE `page` (
  `page_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `page_namespace` int(11) NOT NULL,
  `page_title` varbinary(255) NOT NULL,
  `page_restrictions` tinyblob NOT NULL,
  `page_is_redirect` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `page_is_new` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `page_random` double unsigned NOT NULL,
  `page_touched` binary(14) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  `page_links_updated` varbinary(14) DEFAULT NULL,
  `page_latest` int(10) unsigned NOT NULL,
  `page_len` int(10) unsigned NOT NULL,
  `page_content_model` varbinary(32) DEFAULT NULL,
  `page_lang` varbinary(35) DEFAULT NULL,
  PRIMARY KEY (`page_id`),
  UNIQUE KEY `name_title` (`page_namespace`,`page_title`),
  KEY `page_random` (`page_random`),
  KEY `page_len` (`page_len`),
  KEY `page_redirect_namespace_len` (`page_is_redirect`,`page_namespace`,`page_len`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=binary;

DROP TABLE IF EXISTS `revision`;
CREATE TABLE `revision` (
  `rev_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `rev_page` int(10) unsigned NOT NULL,
  `rev_text_id` int(10) unsigned NOT NULL,
  `rev_comment` varbinary(767) NOT NULL,
  `rev_user` int(10) unsigned NOT NULL DEFAULT '0',
  `rev_user_text` varbinary(255) NOT NULL DEFAULT '',
  `rev_timestamp` binary(14) NOT NULL DEFAULT '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
  `rev_minor_edit` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `rev_deleted` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `rev_len` int(10) unsigned DEFAULT NULL,
  `rev_parent_id` int(10) unsigned DEFAULT NULL,
  `rev_sha1` varbinary(32) NOT NULL DEFAULT '',
  `rev_content_model` varbinary(32) DEFAULT NULL,
  `rev_content_format` varbinary(64) DEFAULT NULL,
  PRIMARY KEY (`rev_id`),
  UNIQUE KEY `rev_page_id` (`rev_page`,`rev_id`),
  KEY `rev_timestamp` (`rev_timestamp`),
  KEY `page_timestamp` (`rev_page`,`rev_timestamp`),
  KEY `user_timestamp` (`rev_user`,`rev_timestamp`),
  KEY `usertext_timestamp` (`rev_user_text`,`rev_timestamp`),
  KEY `page_user_timestamp` (`rev_page`,`rev_user`,`rev_timestamp`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=binary MAX_ROWS=10000000 AVG_ROW_LENGTH=1024;
SELECT * FROM bitnami_mediawiki.text;

DROP TABLE IF EXISTS `text`;
CREATE TABLE `text` (
  `old_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `old_text` mediumblob NOT NULL,
  `old_flags` tinyblob NOT NULL,
  PRIMARY KEY (`old_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=binary MAX_ROWS=10000000 AVG_ROW_LENGTH=10240;


'''


def escapeSQL(string):
    newstr = string.replace('\\','\\\\')\
            .replace('"','\\"')\
            .replace('\'','\\\'')\
            .replace('\u0000','\\0')\
            .replace('\n','\\n')\
            .replace('\r','\\r')\
            .replace('\u001a','\\Z')
    return "'%s'" % newstr


class MWDump:
    def __init__(self, input_file, output_function):
        self.ETip = ET.iterparse(input_file,events=("start","end"))
        first = next(self.ETip)[1]
        self.xmlns = first.tag[:-len('mediawiki')]
        self.stack = []

        self.page = {}
        self.rev = {}
        self.latest_rev = {}
        self.output_function = output_function()

    def run(self):
        for ev, el in self.ETip:
            cleanTag = el.tag[len(self.xmlns):]

            if ev == 'start':
                if cleanTag == 'contributor':
                    self.rev['user'] = 0
                    self.rev['user_text'] = "''"
                    self.stack.append('contributor')
                elif cleanTag == 'revision':
                    self.stack.append('revision')
                elif cleanTag == 'page':
                    self.stack.append('page')

            try:
                level = self.stack[-1]
            except:
                level = None

            if level == 'contributor':
                if cleanTag == 'contributor' and ev == 'end':
                    self.stack.pop()
                elif el.text:
                    if cleanTag == 'id':
                        self.rev['user'] = el.text
                    elif cleanTag in ['username','ip']:
                        self.rev['user_text'] = el.text

            elif level == 'revision':
                if cleanTag in ['minor', 'deleted']:
                    self.rev[cleanTag] = 1
                elif el.text and cleanTag in ['id', 'parentid', 'timestamp',
                        'comment', 'text', 'parentid', 'sha1', 'model', 'format']:
                    self.rev[cleanTag] = el.text

                elif ev == 'end' and cleanTag == 'revision':

                    # prepare data
                    if not 'comment' in self.rev:
                        self.rev['comment'] = ""
                    if not 'text' in self.rev:
                        self.rev['text'] = ""
                    if not 'parentid' in self.rev:
                        self.rev['parentid'] = 'NULL'

                    self.rev['sha1'] = "'%s'" %(self.rev['sha1']) if 'sha1' in self.rev else "''"
                    self.rev['model'] = "'%s'" %(self.rev['model']) if 'model' in self.rev else "NULL"
                    self.rev['format'] = "'%s'" %(self.rev['format']) if 'format' in self.rev else "NULL"
                    self.rev['page'] = self.page['id']
                    self.rev['minor'] = 1 if 'minor' in self.rev else 0
                    self.rev['deleted'] = 0 if 'deleted' in self.rev else 0
                    self.rev['timestamp'] = parse(self.rev['timestamp']).strftime('%Y%m%d%H%M%S')

                    # latest rev
                    if 'timestamp' not in self.latest_rev or self.latest_rev['timestamp'] < self.rev['timestamp']:
                        self.latest_rev = self.rev

                    self.output_function.run('revision', self.rev)

                    self.rev = {}
                    self.stack.pop()


            elif level == 'page':
                if cleanTag in ['redirect']:
                    self.page[cleanTag] = 1
                elif el.text and cleanTag in ['id','ns','title','restrictions']:
                    if cleanTag == 'title':
                        try:
                            title = escapeSQL(el.text.replace(" ","_"))
                            splitsies = title.split(":",1)
                            if splitsies[0] in ["'"+x for x in wiki_namespaces]:
                                title = "'"+splitsies[1]
                        except:
                            title = ""
                        self.page['title'] = title

                    else:
                        self.page[cleanTag] = el.text

                elif ev == 'end' and cleanTag == 'page':
                    self.page['random'] = random()
                    self.page['touched'] = datetime.now().strftime('%Y%m%d%H%M%S')
                    self.page['latest_rev'] = self.latest_rev['id']
                    self.page['latest_rev_len'] = len(self.latest_rev['text'])

                    self.page['redirect'] = 1 if 'redirect' in self.page else 0
                    if not 'restrictions' in self.page:
                        self.page['restrictions'] = ""

                    self.output_function.run('page', self.page)
                    self.stack.pop()
                    self.page = {}
                    self.latest_rev = {}

            el.clear()
        self.output_function.end()


class MySQL_Output:
    class MyPrint:
        def __init__(self):
            self.limit = 80 * 1024 * 1024
            self.size = 0
            uprint('BEGIN;')
        def do(self, text):
#            self.size += len(text)
#            if self.size > self.limit:
#                uprint('COMMIT;\nBEGIN;')
#                self.size = 0
            uprint(text)


    class SQLInsertLineBuffer:
        limit = 800 * 1024
        def __init__(self, print_function, sql_statement):
            self.statement = sql_statement
            self.size = 0
            self.array = []
            self.print_function = print_function

        def add(self, mydata):
            self.size += len(mydata)
            if self.size > self.limit:
                self.doprint()
            self.array += [mydata]

        def doprint(self):
            sql = self.statement % (",".join(self.array))
            self.print_function(sql)

            self.size = 0
            self.array = []

        def finish(self):
            self.doprint()

    def __init__(self):
        myprint = self.MyPrint()
        self.text = self.SQLInsertLineBuffer( myprint.do,
            "INSERT INTO text (old_id,old_text,old_flags) VALUES %s;")
        self.rev = self.SQLInsertLineBuffer( myprint.do,
            "INSERT INTO revision (rev_id,rev_page,rev_text_id,rev_comment,rev_user,rev_user_text,rev_timestamp,rev_minor_edit,rev_deleted,rev_parent_id,rev_sha1,rev_content_model,rev_content_format) VALUES %s;")
        self.page = self.SQLInsertLineBuffer( myprint.do,
            "INSERT INTO page (page_id,page_namespace,page_title,page_is_redirect,page_random,page_touched,page_latest,page_len,page_restrictions) VALUES %s;")


    def run(self, mytype, mydata):
        if mytype == 'page':
            page = mydata
            self.page.add("(%s,%s,%s,%s,%s,%s,%s,%s,%s)" %(
                page['id'], page['ns'], page['title'], page['redirect'],
                page['random'], page['touched'],
                page['latest_rev'], page['latest_rev_len'],
                escapeSQL(page['restrictions']))
                )

        elif mytype == 'revision':
            rev = mydata
            self.text.add( "(%s,%s,'utf-8')" % (
                    rev['id'], escapeSQL(rev['text']) )
                )
            self.rev.add( "(%s,%s,%s,%s,%s,%s,'%s',%s,%s,%s,%s,%s,%s)" % (
                rev['id'], rev['page'], rev['id'], escapeSQL(rev['comment']),
                rev['user'], escapeSQL(rev['user_text']), rev['timestamp'],
                rev['minor'], rev['deleted'], rev['parentid'], rev['sha1'],
                rev['model'], rev['format'])
                )

    def end(self):
        uprint(TABLE_STRUCTURE)
        self.page.finish()
        self.text.finish()
        self.rev.finish()
        uprint('COMMIT;')


if len(sys.argv) > 1:
    filename = sys.argv[1]
else:
    filename = sys.stdin.readline()[:-1]


mwd = MWDump(filename, MySQL_Output)
mwd.run()

