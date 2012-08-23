#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, re, json, codecs
import cProfile
#def get_doc_to_commitee_assoc():
#    """
#    the knesset website tags documents by committees, scrape it
#    only major committees appear, so this is not used to tag by committee
#    """
#    h=urllib.urlopen('http://www.knesset.gov.il/mmm/heb/MMM_Committee_Search.asp').read()
#    s=bs4(h)
#    links={x.text : 'http://www.knesset.gov.il/mmm/heb/'+x.attrs['href'] for x in s.find_all("a","Link2")}
#
#    docsMap={}
#    for (k,v) in links.iteritems():
#        logger.info("retrieving %s" % v)
#        m=MetaDataRetriever(v)
#        data=m.getData()
#        logger.info("Found %d docs" % len(data))
#        for x in data:
#            docId = utils.get_base_name(x['url']),
#            commList=docsMap.get(docId,[])
#            commList.append(k)
#            docsMap[docId]=commList
#
#    return docsMap
#
#def find_committee_slugs(datadict):
#    # hopefully, the first word in the name of the committee
#    # is a unique identifier. This will rear it's ugly head someday
#    commitees = []
#    for (k, v) in datadict.iteritems():
#        for c in v['candidates']:
#            m = re.search(u"ועד[הת](\s*[^\s,\(\)\\.]+){1,6}", c)
#            if m:
#                commitees.append(m.group(0))
#
#    logger.info("%d unique committees found, %d total occurrences", len(set(commitees)), len(commitees))
#    for k in sorted(set(commitees)):
#        logger.info("committee: %s " % k)
#
#