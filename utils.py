#!/usr/bin/env python
# -*- coding: utf-8 -*-


import codecs
import os
import re

# quite LISPy this...
def merge_lines(fname,n=2,sep=" ",func=str.strip,pats=[]):
	"""
	same as merge_lines_g, but not lazy
	"""

	return [x for g in merge_lines_g(fname,n,sep,func,pats)]

# quite LISPy this...
def merge_lines_g(fname,n=2,sep=" ",func=str.strip,pats=[]):
	"""
	takes a file name and return a generator which returns string constructed
	by taking each successive n lines in the file, applying func  to each lines
	(strip by default), and then joining them using sep. all joined using sep.
	when reaching the end of the files, the last lines are joined
	with "" instead of the "missing" n-1 lines pass the end.
	"""

	f =open(fname)
	lines =[]

	for i in range(n):
		lines.append(f.readline())

	while (lines[0] != ''):
		l= sep.join(map(func,lines))
		l=l.decode('utf-8')
		for (s_pat,rep_pat) in pats:
			l=re.sub(s_pat,rep_pat,l)

		lines= lines[1:]
		lines.append(f.readline())

		yield l

#g=merge_lines_g("../crowd-source/app/static/docs/all.txt",n=3,
#	#g=merge_lines_g("1.txt",n=3,
#	pats=[['"',""],[u"[^א-ת\d\s]",u" "],[u"\s+",u" "]])
#with codecs.open("all-2lines.txt","wb",encoding='utf-8') as f:
#	for l in g:
#		f.write(l+"\n")

#dumpListToFile(json.load(open("mks.json")),"mks.txt")
#json.dump(getListFromFile("mks.txt"),open("mks.json","wb"))
def dumpListToFile(l,oname):
	"""
	helper function for editing the mks.json file:
	# dumpListToFile(json.load(open("mks.json")),"mks.txt")
	will dump the contents to a csv-type file for easy editing
	use getListFromToFile with json.dump to save out the updated ver
	don't forget to delete the speaker cache
	"""
	with codecs.open(oname,"wb",encoding='utf-8') as f:
		for (k,v) in l:
			f.write("%s\t%s\n" % (k,v))

def getListFromFile(iname):
	"""
	helper function for editing the mks.json file:
	#json.dump(getListFromFile("mks.txt"),open("mks.json","wb"))
	will dump the contents of a csv type file into a json file.
	don't forget to delete the speaker cache
	"""
	with open(iname) as f:
		lines=f.readlines()
		return [x.strip().split("\t") for x in lines]


def heb_month_into_number(mstr):
	"""
	january-decdember become 1-12
	"""
	months = [u"monthzero",u"ינואר", u"פברואר", u"מר[סצץ]",
	          u"אפריל", u"מאי", u"יוני",
	          u"יולי", u"אוגוסט", u"ספטמבר",
	          u"אוקטובר", u"נובמבר", u"דצמבר"]

	for (i,m) in enumerate(months):
		if re.search(m,mstr):
			return i

	return None