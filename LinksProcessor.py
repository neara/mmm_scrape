#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import Counter

import sys,os,re,json,codecs
import cProfile
from scrape_mmm import logger
import settings
import utils

def main():
    pass
    
if __name__=='__main__':
#	cProfile.run('main()','out.prof')
	main()

class LinksProcessor(object):
    data = None
    filterDate = None

    def __init__(self, data=None, filterDate=settings.START_DATE):
        if filterDate:
            self.filterDate = filterDate
        else:
            self.filterDate = None

        if data == None:
            with codecs.open(settings.LINKSFILE, encoding='utf-8') as f:
                self.data = json.load(f)
        else:
            self.data = data


    def deDupe(self, data):
        logger.info("Deduplicating links")
        # each document appears once for each author, so we need to distinct
        cnt = Counter(x['url'] for x in self.data)
        dupes = filter(lambda x: x[1] > 1, cnt.iteritems())

        # convert to dict keyed by URL
        datadict = {d['url']: d for d in self.data}

        # since duplication squashes some rows, join the authors into
        # a single list in the output file

        # each entry has an authors key which initially hods a singleton list of authors
        # for dupes, we coalesce these into a single list with multiple entries
        # nice and consistent.
        for key in dict(dupes).keys():
            datadict[key]['authors'] = reduce(lambda x, y: x + y, [x['authors'] for x in self.data if x['url'] == key])

        logger.info(
            "%d documents have dupes, for a total of %d duplicates" % (len(dupes), sum([x[1] - 1 for x in dupes])))
        return datadict

    def filterByDate(self, datadict):
        if self.filterDate:
            datadict = {k: v for (k, v) in datadict.iteritems() if
                        utils.asciiDateToDate(v['date']) >= utils.asciiDateToDate(self.filterDate)}
            logger.info(
                "only %d documents published after %s will be processed." % (len(datadict), self.filterDate))
        return datadict

    def getData(self):
        d = self.deDupe(self.data)
        d = self.filterByDate(d)
        return d