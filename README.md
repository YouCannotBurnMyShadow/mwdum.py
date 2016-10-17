mwdum.py
========

MWDumpy takes a Mediawiki-XML-Dump and outputs SQL statements



I built this script due to frustration with mwdumper.
It's currently slower than mwdumper, however it doesn't throw nasty UTF-8 Exceptions. Speedups are on the roadmap for v0.2.
Plus the source code is super slim and easily hackable.

## Usage:

` python3 mwdum.py enwiki-latest-pages-articles.xml > enwiki-latest-pages-articles.sql `

## Improvements:

Table structure for tables: 'page', 'revision' and 'text'.