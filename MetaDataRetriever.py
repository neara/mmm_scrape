#!/usr/bin/env python
# -*- coding: utf-8 -*-
# BSD.
# Copyright (c) 2012, y-p (repos: http://github.com/y-p)
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
from bs4 import BeautifulSoup as bs4

import sys,os,re,json,codecs
import cProfile
import urllib
from scrape_mmm import logger
import settings

def main():
    pass
    
if __name__=='__main__':
#	cProfile.run('main()','out.prof')
	main()

class MetaDataRetriever(object):
    linksOutputFile = None
    url = None
    links = None

    def __init__(self, url="http://knesset.gov.il/mmm/heb/MMM_Results.asp"
                 , linksOutputFile=None):
        self.url = url
        self.linksOutputFile = linksOutputFile

    def extract_docs_from_soup(self, s):
        logger.info("Extracting document metadata")
        d_links = filter(lambda x: x['href'].find("/pdf/") >= 0, s.find_all("a", "Link3"))
        d_links = ['http://knesset.gov.il' + x['href'] for x in d_links]
        d_titles = [x.text for x in s.find_all("td", "Title2")]
        d_body = s.find_all("td", "Text13")
        d_date = [x.text for x in [a.find_all("font")[0] for a in d_body]]
        d_author = [x.text for x in [a.find_all("font")[1] for a in d_body]]

        if not len(d_links) == len(d_titles) == len(d_date) == len(d_author):
            print "Had trouble processing the data from the page. dying"
            sys.exit(1)

        data = zip(d_titles, d_links, d_date, d_author)
        data = [{'title': d[0], 'url': d[1], 'date': d[2], 'authors': [d[3]]} for d in data]
        self.links = data

    def scrape(self):
        """ get the page, extract the data, return a list of dicts"""
        """ with keys 'title','url','date' and 'author'"""

        logger.info("Retrieving  %s" % self.url)
        h = urllib.urlopen(self.url).read()
        logger.info("Parsing HTML")
        s = bs4(h)

        self.extract_docs_from_soup(s)
        del s
        return self

    def save(self):
        if self.linksOutputFile:
            with codecs.open(self.linksOutputFile, "wb", encoding='utf-8') as f:
                json.dump(self.links, f, indent=2)
                logger.info("saved data on documents as json in %s", settings.LINKSFILE)
        else:
            logger.warning("Could not save links to file, no filename provided to constructor")
        return self

    def getData(self):
        return self.links