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

import codecs
import os
import re

# quite LISPy this...
import datetime
import settings

def mergeLines(lines, n=3):
    """
    takes lines in groups of n and process them as a single block of text
    this handles cases where names are split over lines, and some
    other corner cases
    """
    for i in range(min(settings.NUMLINES_TO_PROCESS, len(lines) - n)):
        yield " ".join([lines[i + j].strip() for j in range(n)])


#def merge_lines(fname, n=2, sep=" ", func=str.strip, pats=[]):
#    """
#     same as merge_lines_g, but not lazy
#     """
#
#    return [x for g in merge_lines_g(fname, n, sep, func, pats)]
#
## quite LISPy this...
#def merge_lines_g(fname, n=2, sep=" ", func=str.strip, pats=[]):
#    """
#     takes a file name and return a generator which returns string constructed
#     by taking each successive n lines in the file, applying func  to each lines
#     (strip by default), and then joining them using sep. all joined using sep.
#     when reaching the end of the files, the last lines are joined
#     with "" instead of the "missing" n-1 lines pass the end.
#     """
#
#    f = open(fname)
#    lines = []
#
#    for i in range(n):
#        lines.append(f.readline())
#
#    while (lines[0] != ''):
#        l = sep.join(map(func, lines))
#        l = l.decode('utf-8')
#        for (s_pat, rep_pat) in pats:
#            l = re.sub(s_pat, rep_pat, l)
#
#        lines = lines[1:]
#        lines.append(f.readline())
#
#        yield l

#g=merge_lines_g("../crowd-source/app/static/docs/all.txt",n=3,
#	#g=merge_lines_g("1.txt",n=3,
#	pats=[['"',""],[u"[^א-ת\d\s]",u" "],[u"\s+",u" "]])
#with codecs.open("all-2lines.txt","wb",encoding='utf-8') as f:
#	for l in g:
#		f.write(l+"\n")

#dumpListToFile(json.load(open("mks.json")),"mks.txt")
#json.dump(getListFromFile("mks.txt"),open("mks.json","wb"))
def dumpListToFile(l, oname):
    """
     helper function for editing the mks.json file:
     # dumpListToFile(json.load(open("mks.json")),"mks.txt")
     will dump the contents to a csv-type file for easy editing
     use getListFromToFile with json.dump to save out the updated ver
     don't forget to delete the speaker cache
     """
    with codecs.open(oname, "wb", encoding='utf-8') as f:
        for (k, v) in l:
            f.write("%s\t%s\n" % (k, v))


def getListFromFile(iname):
    """
     helper function for editing the mks.json file:
     #json.dump(getListFromFile("mks.txt"),open("mks.json","wb"))
     will dump the contents of a csv type file into a json file.
     don't forget to delete the speaker cache
     """
    with open(iname) as f:
        lines = f.readlines()
        return [x.strip().split("\t") for x in lines]


def heb_month_into_number(mstr):
    """
     january-decdember become 1-12
     """
    months = [u"monthzero", u"ינואר", u"פברואר", u"מר[סצץ]",
              u"אפריל", u"מאי", u"יוני",
              u"יולי", u"אוגוסט", u"ספטמבר",
              u"אוקטובר", u"נובמבר", u"דצמבר"]

    for (i, m) in enumerate(months):
        if re.search(m, mstr):
            return i

    return None


def asciiDateToDate(x):
    """
    parse date of the form "dd/mm/YYYY"
    """
    datel = x.split("/")
    datel.reverse()
    return apply(datetime.date, map(int, datel))

def get_base_name(url):
    return url.split("/")[-1].split(".")[0]

def reverse_nums(line):
    text=line
    for m in re.finditer("\d+",line):
        s=m.start()
        e=m.end()
        rev="".join(reversed(list(text[m.start():m.end()])))
        text=text[0:s]+rev+text[e:]

    return text