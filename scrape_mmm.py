#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Best-effort scraper for mmm documents from http://knesset.gov.il/mmm/heb/MMM_Results.asp
# retrieves all files to to local cache, converts them to text and tries to
# identify the identities (mks, commitees) referenced as the solicitor for each report.
#
# deps: runs on linux, pdftotext must be installed, and the python packages bs4
#       and fuzzywuzzy  (https://github.com/seatgeek/fuzzywuzzy), pyyaml
#
# dumps out several files into "output_data/:
# - the document meta data scraped from the webpage goes in LINKSFILE
# - the metadata for documents which matched with a score > SCORE_THRESHOLD
#   goes in MATCHESFILE
# - a count of docments per mk is dumped in csv form into COUNTS_CSVFILE
#
# the documents go back to 2000, but hte entity.yaml file only holds
# mks from the 18th knesset right now.
#
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
from functools import partial

import  json, codecs
import re
import multiprocessing
from collections import Counter
from DocumentCache import DocumentCache
from LinksProcessor import LinksProcessor
from MetaDataRetriever import MetaDataRetriever
from dump_matches_to_tsv import write_tsv, data_to_gen, filter_keys
from extract_session_dates import *
from fuzzywuzzy import fuzz
import logging
from jinja2 import Template
import yaml
import settings
import utils

########################################################
# Globals
########################################################

MAGIC_RE = u"((מסמך)\s+זה)" +\
           u"|((הוכן|מוגש|נכתב)\s+(עבור|לכבוד|לבקשת|לקראת|למען|בשביל))" +\
           u"|((לקראת|עקבות)\s+(דיו[נן]|פגישה|ישיבה))" +\
           u"|(לכבוד|בראשות).+(חברת? הכנסת|חהכ)" +\
           u"|לכבוד.+ועד" +\
           u"|(לבקשת|מוגש).+ועד" +\
           u"|דיו[נן]\s+משות"
DATE_RE = u"(מסמך\s+זה).+(הוכן|מוגש|נכתב).+(דיו[ןנ]|ישיב|פגיש).+(ינואר|פברואר|מרץ|מרס|מארס|אפריל|מאי|יוני|יולי|אוגוסט|ספטמבר|אוקטובר|נובמבר|דצמבר)"
TOPIC_RE = u"(לקראת\s+דיון\s+בוו?עד).+(בנושא|כותרתה)"

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s %(name)-4s %(levelname)-8s %(message)s',
    datefmt='%m-%d %H:%M:%S',
    #		    filename='/tmp/myapp.log',
    #		    filemode='w'
)
logger = logging.getLogger("mmm-scrape")

# IDENTITIESYAMLFILE should contain mappings from names
# to ids (the included file maps names to mk  ids as they appear on oknesset.org)
#
# can't hide the global inside a class
# because multiprocessing.Map chokes when given a method
# instead of a function

with open(settings.IDENTITIESYAMLFILE, "rb") as f:
    identities = list(yaml.load(f).iteritems())

########################################################
# Code
########################################################


def score(score_threshold, d):
    """ fuzzy match between all the records in identities """
    """ and all the lines present inside d['candidates'] """
    results = []
    for heading in d['candidates']:
        cand = [{'docid': utils.get_base_name(d['url']),
                 'url': d['url'],
                 'title': d['title'],
                 'date': d['date'],
                 'score': 0 if len(heading) < 6 else fuzz.partial_ratio(entityName, heading),
                 'entityName': entityName,
                 'id': id,
                 'heading': heading}    for (entityName, id) in identities]

        results.append([x for x in cand if x['score'] > score_threshold])
    return results

def createRankings(matches):
    """ build a table which holds the number of documents associated with each name"""

    logger.info("Preparing rankings")
    cnt = list(Counter([x['entityName'] for x in matches]).iteritems())
    cnt.sort(key=lambda x: x[1])

    # output an excel-compatible CSV of ranking
    with codecs.open(settings.COUNTS_CSVFILE, "wb", encoding='utf-16-le') as f:
        for (name, count) in cnt:
            f.write(u"%s\t%d\n" % (name, count))

def makeMatches(datadict):
    logger.info("Scoring %d documents " % len(datadict))
    # use all cores to do the scoring, this is O(no. of identities * number of lines with magic pattern)
    # can take a little while.
    p = multiprocessing.Pool(multiprocessing.cpu_count())
    f = partial(score, settings.SCORE_THRESHOLD)
    matches = p.map(f, datadict.values())
    matches = reduce(lambda x, y: x + y, matches)
    matches = reduce(lambda x, y: x + y, matches)

    matchdict = {}
    for v in matches:
        # deduplicate hits by storing in a dict keyed by "docid-entityid"
        key = "%s-%s" % (v['docid'], v['id'])
        matchdict[key] = matchdict.get('key', []) + [v]

    return matchdict

def findNotMatched(datadict,matches):
    # save data of orphan documents separately, for forensics.
    not_matched = {x['url'] for x in datadict.values()}.difference({x['url'] for x in matches})
    not_matched = [x for x in datadict.values() if x['url'] in not_matched]
    for (i, v) in enumerate(not_matched):
        not_matched[i].update({'docid': utils.get_base_name(v['url'])})
    return not_matched

def dump_json(data,fname):
    with codecs.open(fname, "wb", encoding='utf-8') as f:
        json.dump(data, f, indent=2)

def dump_report(data,fname,tmplf):
    # dump out a summary html file of files which did not match. with links, for review
    with codecs.open(fname, "wb", encoding='utf-8') as f:
        t = Template(codecs.open(tmplf, encoding='utf-8').read())
        logger.info("dumping out html report of %d entries to %s", len(data), fname)
        s = t.render({'objs': sorted(data, key=lambda x: x['url'])})
        f.write(s)

def main():
    data = None

    ##################################
    # get stuff
    ##################################
    retriever = MetaDataRetriever(linksOutputFile=settings.LINKSFILE)
    data=retriever.scrape().save().getData()
    # dedupe, date filter and get back the data
    datadict = LinksProcessor(data, filterDate=settings.START_DATE).getData()

    # retrieve docs from net, convert them to text and cache the result
    docs = DocumentCache(datadict)

    datedict = {}

    ##################################
    # find stuff
    ##################################
    logger.info("Finding candidate lines...")

    for k in sorted(datadict.keys()):
        lines = docs.getDocumentLines(k)

        pat = [x for x in utils.mergeLines(lines,3) if re.search(MAGIC_RE, x)]
        datepat = [x for x in utils.mergeLines(lines,3) if re.search(DATE_RE, x)]

        datadict[k]['candidates'] = pat
        datedict[k] = {'candidates': datepat}

    matchdict=makeMatches(datadict)

    # as a kludge , committees are stored with id COMMITEE_ID_BASE+offset in identities.json
    # so we can separate the matches into types
    mksMatchesCnt = len([x for x in matchdict.values() if int(x[0]['id']) < settings.COMMITEE_ID_BASE])
    commMatchesCnt = len(matchdict) - mksMatchesCnt
    logger.info("Located %d unique matches with score > %d (%d: mks, %d: committee) " %\
                (len(matchdict), settings.SCORE_THRESHOLD, mksMatchesCnt, commMatchesCnt))

    ##################################
    # save stuff
    ##################################
    matches = reduce(lambda x, y: x + y, matchdict.values())
    not_matched=findNotMatched(datadict,matches)

    dump_json(not_matched,settings.NOMATCHESFILE)
    logger.info("saved details of documents with no matches as json in %s", settings.NOMATCHESFILE)

    dump_report(not_matched,settings.NO_MATCHES_HTML_FILE,settings.NO_MATCHES_TEMPLATE_FILE)
    dump_report(matches,settings.MATCHES_HTML_FILE,settings.MATCHES_TEMPLATE_FILE)

    cnt=0
    logger.info("finding committee session dates")
    matchesDict = {x['docid']: x for x in matches}
    for (k, v) in datedict.iteritems():
        for line in v['candidates']:
            line = utils.reverse_nums(line) # text extraction reverses numbers, RTL thing
            # munge and contort to extract a valid date
            d = extract_date(utils.get_base_name(k), line)
            if d and matchesDict.get(d['docid']):
                cnt +=1
                matchesDict[d['docid']]['comm_session_date'] = d['date'].strftime("%d/%m/%Y")

    logger.info("updated %d documents with a committee session date" % cnt)
    # use the updated dict containing comm_session_date
    # for matches
    matches = matchesDict.values()

    logger.info("saved matches as json in %s", settings.MATCHESFILE)
    dump_json(matches,settings.MATCHESFILE)

    logger.info("saved matches as csv in %s", settings.MATCHES_CSV_FILE)
    # saves matches as csv file
    g=filter_keys(data_to_gen(settings.MATCHESFILE))
    write_tsv(g,settings.MATCHES_CSV_FILE)

    # <-> short-circuit here to skip previous stages

    # load the matches back up
    with codecs.open(settings.MATCHESFILE, "r", encoding='utf-8') as f:
        matches = json.load(f)

    createRankings(matches)

    logger.info("saved rankings in %s", settings.COUNTS_CSVFILE)
    logger.info("Cheers.")

if __name__ == "__main__":
    #get_doc_to_commitee_assoc(): # some commitees are not included in
    # http://www.knesset.gov.il/mmm/heb/MMM_Committee_Search.asp, so - we won't use this
    main()
