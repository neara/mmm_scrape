#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Best-effort scraper for mmm documents from http://knesset.gov.il/mmm/heb/MMM_Results.asp
# retrieves all files to to local cache, converts them to text and crudely tries to
# identify the identities (mks, commitees) referenced as the solicitor of the report.
#
# deps: runs on linux, pdftotext must be installed, and the python packages bs4
#       and fuzzywuzzy (https://github.com/seatgeek/fuzzywuzzy)
#
# dumps out 3 files:
# - the document meta data scraped from the webpage goes in LINKSFILE
# - the metadata for documents which matched with a score > SCORE_THRESHOLD
#   goes in MATCHESFILE
# - a count of docments per mk is dumped in csv form into CSVFILE
#
# the documents go back to 2000, but hte identities.json file only holds
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

import urllib, os, json, sys, datetime, codecs
from bs4 import BeautifulSoup as bs4
import re
import multiprocessing
from collections import Counter
from fuzzywuzzy import fuzz
import logging
import subprocess
from jinja2 import Template

#def printd(i):
#    with codecs.open("no_match.json") as f:
#        nm=json.load(f)
#    print i['url'].split("/")[-1]
#    for c in nm[i]['candidates']:
#        print c
from utils import merge_lines_g

COMMITEEJSONFILE = "commitees.json"
MKSJONFILE = "mks.json"
IDENTITIESJSONFILE = "identities.json"
DATADIR = './data/'
SCORE_THRESHOLD = 90
LINKSFILE = "mmm.json"
MATCHESFILE = "matches.json"
CSVFILE = "counts.csv"
COMMITEE_ID_BASE = 10000 # all ids  higher then this in identities.json identify commitees , not persons
NOMATCHESFILE = "no_match.json"
MATCHES_TEMPLATE_FILE = "matches_tmpl.html"
NO_MATCHES_TEMPLATE_FILE = "no_matches_tmpl.html"
MATCHES_HTML_FILE = "matches.html"
NO_MATCHES_HTML_FILE = "no_matches.html"
DATE_TXT_FILE = "dates.txt"
TOPIC_TXT_FILE = "topics.txt"

#MAGIC_RE=u"((מסמך|מכתב|דוח)\s+זה)"+\
MAGIC_RE = u"((מסמך)\s+זה)" +\
           u"|((הוכן|מוגש|נכתב)\s+(עבור|לכבוד|לבקשת|לקראת|למען|בשביל))" +\
           u"|((לקראת|עקבות)\s+(דיו[נן]|פגישה|ישיבה))" +\
           u"|(לכבוד|בראשות).+(חברת? הכנסת|חהכ)" +\
           u"|לכבוד.+ועד" +\
           u"|(לבקשת|מוגש).+ועד" +\
           u"|דיו[נן]\s+משות"
DATE_RE = u"(מסמך\s+זה).+(הוכן|מוגש|נכתב).+(דיו[ןנ]|ישיב|פגיש).+(ינואר|פברואר|מרץ|מרס|מארס|אפריל|מאי|יוני|יולי|אוגוסט|ספטמבר|אוקטובר|נובמבר|דצמבר)"
TOPIC_RE = u"(לקראת\s+דיון\s+בוו?עד).+(בנושא|כותרתה)"

# for shorter runtime, if we only care about documents published since start of k18
# dd/mm/YYYY
#START_DATE="24/02/2012"
START_DATE = "24/02/2009"
#START_DATE=None

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s %(name)-4s %(levelname)-8s %(message)s',
    datefmt='%m-%d %H:%M:%S',
    #		    filename='/tmp/myapp.log',
    #		    filemode='w'
)
logger = logging.getLogger("mmm-scrape")

# IDENTITIESJSONFILE should contain mappings from names
# to ids (the included file maps names to mk  ids as they appear on oknesset.org)
#
# can't hide the global inside a class
# because multiprocessing.Map chokes when given a method
# instead of a function

with open(IDENTITIESJSONFILE, "rb") as jsonfile:
    identities = json.load(jsonfile)

def get_doc_to_commitee_assoc():
    """
    the knesset website tages documents by committees, scrape it
    only major committees appear, so this is not used to tag by committee
    """
    h=urllib.urlopen('http://www.knesset.gov.il/mmm/heb/MMM_Committee_Search.asp').read()
    s=bs4(h)
    links={x.text : 'http://www.knesset.gov.il/mmm/heb/'+x.attrs['href'] for x in s.find_all("a","Link2")}

    docsMap={}
    for (k,v) in links.iteritems():
        logger.info("retrieving %s" % v)
        h=urllib.urlopen(v).read()
        s=bs4(h)
        data=extract_docs_from_soup(s)
        logger.info("Found %d docs" % len(data))
        for x in data:
            docId = get_base_name(x['url']),
            commList=docsMap.get(docId,[])
            commList.append(k)
            docsMap[docId]=commList

    return docsMap

def score(score_threshold, d):
    """ fuzzy match between all the records in identities """
    """ and all the lines present inside d['candidates'] """
    results = []
    for heading in d['candidates']:
        cand = [{'docid': get_base_name(d['url']),
                 'url': d['url'],
                 'title': d['title'],
                 'date': d['date'],
                 'score': 0 if len(heading) < 6 else fuzz.partial_ratio(entityName, heading),
                 'entityName': entityName,
                 'id': id,
                 'heading': heading}    for (entityName, id) in identities]

        results.append([x for x in cand if x['score'] > score_threshold])
    return results


def get_base_name(url):
    return url.split("/")[-1].split(".")[0]

def extract_docs_from_soup(s):
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
    data = [{'title': d[0], 'url': d[1], 'date': d[2]} for d in data]
    return data

def scrape(url):
    """ get the page, extract the data, return a list of dicts"""
    """ with keys 'title','url','date' and 'author'"""

    logger.info("Retrieving  %s" % url)
    h = urllib.urlopen(url).read()
    logger.info("Parsing HTML")
    s = bs4(h)

    return extract_docs_from_soup(s)



def asciiDateToDate(x):
    """
    parse date of the form "dd/mm/YYYY"
    """
    datel = x.split("/")
    datel.reverse()
    return apply(datetime.date, map(int, datel))

# hopefully, the first word in the name of the committee
# is a unique identifier. This will rear it's ugly head someday
def find_committee_slugs(datadict):
    commitees = []
    for (k, v) in datadict.iteritems():
        for c in v['candidates']:
            m = re.search(u"ועד[הת](\s*[^\s,\(\)\\.]+){1,6}", c)
            if m:
                commitees.append(m.group(0))

    logger.info("%d unique commitees found, %d total occurences", len(set(commitees)), len(commitees))
    for k in sorted(set(commitees)):
        logger.info("commitee: %s " % k)

def createRankings(matches):
    # build a table which holds the number of documents associated with each name
    logger.info("Preparing rankings")
    cnt = list(Counter([x['entityName'] for x in matches]).iteritems())
    cnt.sort(key=lambda x: x[1])

    # output an excel-compatible CSV of ranking
    with codecs.open(CSVFILE, "wb", encoding='utf-16-le') as f:
        for (name, count) in cnt:
            f.write(u"%s\t%d\n" % (name, count))

def sanitize_lines(lines):
    lines = [x.decode('utf-8') for x in lines]
    lines = [re.sub(u"['`\"]", "", x) for x in lines]
    lines = [re.sub(u"[^א-ת\d]", " ", x) for x in lines]
    lines = [re.sub(u"וו?עד", u"ועד", x) for x in lines]
    lines = [re.sub(u"[לב]ועד", u"ועד", x) for x in lines]
    #	lines = [re.sub(u"\sה",u" ",x ) for x in lines ]
    lines = [re.sub(u"\s+", " ", x) for x in lines]

    return lines
def main():

    data=scrape("http://knesset.gov.il/mmm/heb/MMM_Results.asp")
    with codecs.open(LINKSFILE,"wb",encoding='utf-8') as f:
        json.dump(data,f)
        logger.info("saved data on documents as json in %s",LINKSFILE)

    # <-> short-circuit here to skip previous stages

    # load back the data
    with codecs.open(LINKSFILE, encoding='utf-8') as f:
        data = json.load(f)

    # convert to dict keyed by URL
    datadict = {d['url']: d for d in data[:]}
    #datadict={d['url']:d for d in data[:]  if d['url'].find("2252")>=0}

    # each document appears once for each author
    # deduplicate
    keys = [x['url'] for x in data] + datadict.keys()
    cnt = Counter(keys)
    dupes = filter(lambda x: x[1] > 2, cnt.iteritems())

    logging.info("%d documents have dupes, for a total of %d duplicates" % (len(dupes), sum([x[1] - 2 for x in dupes])))

    if START_DATE:
        datadict = {k: v for (k, v) in datadict.iteritems() if
                    asciiDateToDate(v['date']) >= asciiDateToDate(START_DATE)}
        logging.info(
            "START_DATE set, only %d documents published after %s will be processed." % (len(datadict), START_DATE))

    datedict = {}
    topicdict = {}
    # retrieve each missing file from the net if needed
    # convert each file to text
    # filter the lines to find thos with the magic pattern
    # save all such lines in d['candidates']
    for (k, d) in sorted(datadict.iteritems()):
        basename = d['url'].split("/")[-1]

        fullpath = os.path.join(DATADIR, basename)
        if not os.path.exists(fullpath):
            logger.info("Retrieving %s into %s" % (d['url'], DATADIR))
            with open(fullpath, "wb") as f:
                f.write(urllib.urlopen(d['url']).read())
                pass
        fulltxtpath = os.path.join(DATADIR, basename.split('.')[0] + ".txt")
        if not os.path.exists(fulltxtpath):
            cmd = "pdftotext -f 1 -l 5 %s -" % fullpath
            logger.info("converting %s to text" % fullpath)

            p = subprocess.Popen(cmd.strip().split(' '), stdout=subprocess.PIPE)
            (contents, errf) = p.communicate()
            with codecs.open(fulltxtpath, "wb", encoding='utf-8') as f:
                f.write(contents.decode('utf-8'))
        else:
            logger.debug("Loading cached text for %s from %s" % (fullpath, fulltxtpath))
            with codecs.open(fulltxtpath, encoding='utf-8') as f:
                contents = f.read().encode('utf-8')

        lines=sanitize_lines(contents.split("\n"))

        # takes lines in groups of n and process them as a single block of text
        # this handles cases where names are split over lines, and some
        # other corner cases
        n = 3 # 2=2 gives 99% , n-3 gets a little more but generates lots of duplicates
        # which substantially slows down the scoring phase
        mergedlines = [" ".join([lines[i + j].strip() for j in range(n)])
                       for i in range(min(1000, len(lines) - n))]

        pat = [x for x in mergedlines if re.search(MAGIC_RE, x)]
        datepat = [x for x in mergedlines if re.search(DATE_RE, x)]
        topicpat = [x for x in mergedlines if re.search(TOPIC_RE, x)]

        datadict[k]['candidates'] = pat
        datedict[k] = {'candidates': datepat}
        topicdict[k] = {'candidates': topicpat}

    ###############################################################################
    # we have now isolated candidate "lines" which might contain the data we want
    # do the scoring and so forth

    #find_committee_slugs(datadict) # this just dumps suspected commitee names to log, used to seed identities.json

    logger.info("Scoring %d documents " % len(datadict))
    # use all cores to do the scoring, this is O(no. of identities * number of lines with magic pattern)
    # can take a little while.
    p = multiprocessing.Pool(multiprocessing.cpu_count())
    f = partial(score, SCORE_THRESHOLD)
    matches = p.map(f, datadict.values())
    matches = reduce(lambda x, y: x + y, matches)
    matches = reduce(lambda x, y: x + y, matches)

    matchdict = {}
    for v in matches:
        # deduplicate hits by storing in a dict keyed by "docid-entityid"
        key = "%s-%s" % (v['docid'], v['id'])
        matchdict[key] = matchdict.get('key', []) + [v]

    # as a kluge , committees are stored with id COMMITEE_ID_BASE+offset in identities.json
    # so we can seperate the matches into types
    mksMatchesCnt = len([x for x in matchdict.values() if int(x[0]['id']) < COMMITEE_ID_BASE])
    commMatchesCnt = len(matchdict) - mksMatchesCnt
    logger.info("Located %d unique matches  with score > %d (%d: mks, %d: committee) " %\
                (len(matchdict), SCORE_THRESHOLD, mksMatchesCnt, commMatchesCnt))


    ##################################
    # save out the various output files

    # dump the good matches to MATCHESFILE

    with codecs.open(MATCHESFILE, "wb", encoding='utf-8') as f:
        json.dump(reduce(lambda x,y:x+y,matchdict.values()), f)
        logger.info("saved matches as json in %s", MATCHESFILE)

    # save data of orphan documents separately, for forensics.
    not_matched = {x['url'] for x in datadict.values()}.difference({x['url'] for x in matches})
    not_matched = [x for x in datadict.values() if x['url'] in not_matched]
    for (i, v) in enumerate(not_matched):
        not_matched[i].update({'docid': get_base_name(v['url'])})

    with codecs.open(NOMATCHESFILE, "wb", encoding='utf-8') as f:
        json.dump(not_matched, f)
        logger.info("saved details of documents with no matches as json in %s", NOMATCHESFILE)

    # dump out a summary html file of matches for review
    with codecs.open(MATCHES_HTML_FILE, "wb", encoding='utf-8') as f:
        t = Template(codecs.open(MATCHES_TEMPLATE_FILE, encoding='utf-8').read())
        logger.info("dumping out html report of matches to %s", MATCHES_HTML_FILE)
        # although multiplte lines may have matched the same entity,
        # and all matches are held in ht ematch entry.
        # there's also a lot of duplication due to  overlap between joined rows
        # so, just show the first.
        s = t.render({'objs': [v[0] for (k, v) in sorted(matchdict.items())]})
        f.write(s)

    # dump out a summary html file of files which did not match. with links, for review
    with codecs.open(NO_MATCHES_HTML_FILE, "wb", encoding='utf-8') as f:
        t = Template(codecs.open(NO_MATCHES_TEMPLATE_FILE, encoding='utf-8').read())
        logger.info("dumping out html report of %d un-matched files to %s", len(not_matched), NO_MATCHES_HTML_FILE)
        s = t.render({'objs': sorted(not_matched, key=lambda x: x['url'])})
        f.write(s)

    # dump out candidates for committee meeting date
    with codecs.open(DATE_TXT_FILE, "wb", encoding='utf-8') as f:
        logger.info("dumping out names of files with probable session dates to %s", DATE_TXT_FILE)
        for (k, v) in datedict.iteritems():
            for x in v['candidates']:
                f.write("%s\t%s" % (get_base_name(k), x) + "\n")

    # dump out candidates for committee meeting topic
    with codecs.open(TOPIC_TXT_FILE, "wb", encoding='utf-8') as f:
        logger.info("dumping out names of files with probable session topics to %s", TOPIC_TXT_FILE)
        for (k, v) in topicdict.iteritems():
            for x in v['candidates']:
                f.write("%s\t%s" % (get_base_na + me(k), x) + "\n")

    # <-> short-circuit here to skip previous stages

    # load the matches back up
    with codecs.open(MATCHESFILE, "r", encoding='utf-8') as f:
        matches = json.load(f)

    createRankings(matches)


    logger.info("saved rankings in %s", CSVFILE)
    logger.info("Cheers.")


if __name__ == "__main__":
    #get_doc_to_commitee_assoc(): # some commitees are not included in http://www.knesset.gov.il/mmm/heb/MMM_Committee_Search.asp
    # don't use
    main()

# are the same file
#http://knesset.gov.il/mmm/data/pdf/m02526.pdf
#http://knesset.gov.il/mmm/data/pdf/m02526.pdf
