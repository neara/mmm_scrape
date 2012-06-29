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


import urllib,os,json,sys,datetime,codecs
from bs4 import BeautifulSoup as bs4
import re
import multiprocessing
from collections import Counter
from fuzzywuzzy import fuzz
import logging
import subprocess

IDENTITIESJSONFILE="identities.json"
DATADIR='./data/'
SCORE_THRESHOLD=90
LINKSFILE="mmm.json"
MATCHESFILE="matches.json"
CSVFILE="counts.csv"
COMMITTE_ID_BASE=10000 # all ids  higher then this in identities.json identify commitees , not persons
NOMATCHESFILE="no_match.json"

MAGIC_RE=u"(מסמך\s+זה)|(נכתב לבקשת)|(לכבוד)|(מוגש)"
# for shorter runtime, if we only care about documents published since start of k18
# dd/mm/YYYY
START_DATE="24/02/2009"
#START_DATE=None

logging.basicConfig(level=logging.INFO,
	format='%(asctime)s %(name)-4s %(levelname)-8s %(message)s',
	datefmt='%m-%d %H:%M:%S',
#		    filename='/tmp/myapp.log',
#		    filemode='w'
)
logger=logging.getLogger("mmm-scrape")

# IDENTITIESJSONFILE should contain mappings from names
# to ids (the included file maps names to mk  ids as they appear on oknesset.org)
#
# can't hide the global inside a class
# because multiprocessing.Map chokes when given a method
# instead of a function

with open(IDENTITIESJSONFILE,"rb") as jsonfile:
	identities=json.load(jsonfile)

def	score(d):
	""" fuzzy match between all the records in identities """
	""" and all the lines present inside d['candidates'] """
	results=[]
	for heading in d['candidates']:
		results.append( [{'url' : d['url'],
		                  'title' : d['title'],
		                  'date' : d['date'],
						  'score' :0 if len(heading)<6 else fuzz.partial_ratio(entityName,heading),
						  'entityName': entityName,
						  'id':id,
						  'heading' : heading}	for (entityName,id) in identities])

	return results

def scrape(url):
	""" get the page, extract the data, return a list of dicts"""
	""" with keys 'title','url','date' and 'author'"""

	logger.info("Retrieving  %s" % url)
	h=urllib.urlopen(url).read()
	logger.info("Parsing HTML")
	s=bs4(h)
	logger.info("Extracting document metadata")
	d_links=filter(lambda x: x['href'].find("/pdf/") >=0,s.find_all("a","Link3"))
	d_links= ['http://knesset.gov.il'+x['href'] for x in d_links]
	d_titles=[x.text for x in s.find_all("td","Title2")]
	d_body=s.find_all("td","Text13")
	d_date=[x.text for x in [a.find_all("font")[0] for a in d_body]]
	d_author=[x.text for x in [a.find_all("font")[1] for a in d_body]]

	if not len(d_links)==len(d_titles)==len(d_date)==len(d_author):
		print "Had trouble processing the data from the page. dying"
		sys.exit(1)

	data=zip(d_titles,d_links,d_date,d_author)
	data=[{'title':d[0],'url':d[1],'date':d[2]} for d in data]

	return data

def asciiDateToDate(x):
	"""
	parse date of the form "dd/mm/YYYY"
	"""
	datel=x.split("/")
	datel.reverse()
	return apply(datetime.date,map(int,datel))

# hopefully, the first word in the name of the committee
# is a unique identifier. This will rear it's ugly head someday
def find_committee_slugs(datadict):
	commitees=[]
	for (k,v) in datadict.iteritems():
		for c in v['candidates']:
			m=re.search(u"ועד[הת](\s*[^\s,\(\)\\.]+){1,6}",c)
			if m:
				commitees.append(m.group(0))

	logger.info("%d unique commitees found, %d total occurences",len(set(commitees)),len(commitees) )
	for k in sorted(set(commitees)):
		logger.info("commitee: %s " % k)


def main():
	data=scrape("http://knesset.gov.il/mmm/heb/MMM_Results.asp")
	with codecs.open(LINKSFILE,"wb",encoding='utf-8') as f:
		json.dump(data,f)
		logger.info("saved data on documents as json in %s",LINKSFILE)

	# <-> short-circuit here to skip previous stages

	# load back the data
	with codecs.open(LINKSFILE,encoding='utf-8') as f:
			data=json.load(f)

	# convert to dict keyed by URL
	datadict={d['url']:d for d in data[:]}

	keys=[x['url'] for x in data]+datadict.keys()
	cnt=Counter(keys)
	dupes=filter(lambda x: x[1]>2,cnt.iteritems())

	logging.info("%d documents have dupes, for a total of %d duplicates" % (len(dupes),sum ([x[1]-2 for x in dupes])))

	if START_DATE :
		datadict={k:v for (k,v) in datadict.iteritems() if asciiDateToDate(v['date']) >= asciiDateToDate(START_DATE)}
		logging.info("START_DATE set, only %d documents published after %s will be processed." % (len(datadict),START_DATE))



	# retrieve each missing file from the net if needed
	# convert each file to text
	# filter the lines to find thos with the magic pattern
	# save all such lines in d['candidates']
	for (k,d) in  sorted(datadict.iteritems()):
		basename=d['url'].split("/")[-1]

		fullpath=os.path.join(DATADIR,basename)
		if not os.path.exists(fullpath):
			logger.info("Retrieving %s into %s" % (d['url'],DATADIR))
			with open(fullpath,"wb") as f:
				f.write(urllib.urlopen(d['url']).read())
				pass
		fulltxtpath=os.path.join(DATADIR,basename.split('.')[0]+".txt")
		if not os.path.exists(fulltxtpath):
			cmd = "pdftotext -f 1 -l 5 %s -" % fullpath
			logger.info("converting %s to text" % fullpath)

			p = subprocess.Popen(cmd.strip().split(' '), stdout=subprocess.PIPE)
			(contents, errf) = p.communicate()
			with codecs.open(fulltxtpath,"wb",encoding='utf-8') as f:
				f.write(contents.decode('utf-8'))
		else:
			logger.info("Loading cached text for %s from %s" % (fullpath,fulltxtpath))
			with codecs.open(fulltxtpath,encoding='utf-8') as f:
				contents=f.read().encode('utf-8')

		lines=[x.decode('utf-8') for x in contents.split("\n")]
		pat = [ lines[i].strip() + " " + lines[i+1].strip()
		        for (i,x) in enumerate(lines[:max(1000,len(lines)-2)]) if re.search(MAGIC_RE,x)]

		datadict[k]['candidates']=pat


	find_committee_slugs(datadict)

	logger.info("Scoring %d documents " % len(datadict))
	# use all cores to do the scoring, this is O(no. of identities * number of lines with magic pattern)
	# can take a little while.
	p=multiprocessing.Pool(multiprocessing.cpu_count())
	scores=p.map(score,datadict.values())
	scores=reduce(lambda x,y:x+y,scores)


	matches=[]
	for v in scores:
		# keep all high enough scores
		best=[ e for e in v if e['score'] > SCORE_THRESHOLD]
		for c in best:
			if (c['score'] > SCORE_THRESHOLD):
				matches.append(c)


	logger.info("Located %d matches to with score > %d (%d: mks, %d: committee) " %\
	            (len(matches), SCORE_THRESHOLD,
	             len([x for x in matches if int(x['id']) < COMMITTE_ID_BASE]),
	             len([x for x in matches if int(x['id']) >= COMMITTE_ID_BASE])))

	# dump the good matches to MATCHESFILE
 	with codecs.open(MATCHESFILE,"wb",encoding='utf-8') as f:
		json.dump(matches,f)
		logger.info("saved matches as json in %s",MATCHESFILE)

	# save data of orphan documents separately, for forensics.
	not_matched={x['url'] for x in datadict.values()}.difference({x['url'] for x in matches})
	not_matched=[x for x in datadict.values() if x['url'] in not_matched ]
	with codecs.open(NOMATCHESFILE,"wb",encoding='utf-8') as f:
		json.dump(not_matched,f)
		logger.info("saved details of documents with no matches as json in %s",NOMATCHESFILE)


	# <-> short-circuit here to skip previous stages

	# load it back up
	with codecs.open(MATCHESFILE,"r",encoding='utf-8') as f:
		matches=json.load(f)

	# build a table which holds the number of documents associated with each name
	logger.info("Preparing rankings")
	cnt= list(Counter ([x['entityName'] for x in matches]).iteritems())
	cnt.sort(key=lambda x:x[1])

	# output an excel-compatible CSV of ranking
	with codecs.open(CSVFILE,"wb",encoding='utf-16-le') as f:
		for (name,count) in cnt:
			f.write(u"%s\t%d\n" % (name,count))

	logger.info("saved rankings in %s",CSVFILE)

	logger.info("Cheers.")

if __name__ == "__main__":
	main()
