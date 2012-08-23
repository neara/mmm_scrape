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

import sys, os, re, json, codecs
import logging
import datetime
from scrape_mmm import *

lfmt = logging.Formatter(fmt='%(asctime)s %(name)-4s %(levelname)-8s %(message)s',
    datefmt='%m-%d %H:%M:%S'
)

sh = logging.StreamHandler()
sh.setFormatter(lfmt)

logger=logging.getLogger("extract_session")
logger.addHandler(sh)
logger.setLevel(logging.INFO)


# code

def heb_month_into_number(mstr):
    u"""
    january-decdember become 1-12

    >>> heb_month_into_number(u"אוקטובר")
    10

    >>> heb_month_into_number(u"אוטובר")
    10

    """
    months = [u"monthzero", u"ינואר", u"פברואר", u"מא?ר[סצץ]",
              u"אפריל", u"מאי", u"יוני",
              u"יולי", u"אוגוסט", u"ספטמבר",
              u"אוקטובר", u"נובמבר", u"דצמבר"]

#    if re.match("\d+",unicode(mstr)):
#        return int(mstr)

    for (i, m) in enumerate(months):
        if re.search(m, mstr):
            return i

    return None


def extract_commitee_and_date():
    ls=codecs.open("dates.txt",encoding="utf-8").readlines()
    ls=[x for x in ls if re.search(u"(לקראת|עקבות|מוגש|הוכן|נכתב).*(ישיב|דיון|פגיש)",x)>=0]



def select_lines(ls):
    return [x for x in ls if re.search(u"(לקראת|עקבות|מוגש|הוכן|נכתב).*(ישיב|דיון|פגיש)",x)>=0]

def handle_date(d,m,y):

    # try and autofix some pathologies, often other parts of the date are also mangled
    # so this won't help much
    yearmap={}#"0227":"2007","3122": "2011", "2112" : "2012", "0211" : "2011", "3202": "2003", "0202": "2002","0292":"2009","2102":"2012","1222":"2012","0225":"2005","6202":"2006","2122": "2012","8202":"2008"}

    if yearmap.get(y):
        y=yearmap.get(y)
        d="".join(reversed(list(d))) # heuristic: when year is messed up, the day is reversed


    if not re.match(u"^\d+$",m):
        m=re.sub(u"^[ב]","",m)
        m=heb_month_into_number(m)

    try:
        d=int(d);m=int(m);y=int(y)
    except:
        return None

    if m and d<31 and 2000 < y < 2019 :
        return datetime.date(y,m,d)
    else:
        return None

def date_by_docid_override(docid):
    date=None
    if docid=="m03018": #
        date= "1/7/2009"

    if docid=="m02598":
        date= "18/5/2009"

    if docid=="m02651":
        date="27/10/2010"

    if docid == "m02706": #  pathology
        date="1/12/2010"

    if docid == "m03003" : #   pathology
        date="2/1/2012"

    if docid=="m02191" : #
        date="17/3/2009"

    if docid=="m03017":
        date="24/1/2011"

    if docid=="m02945":
        date="1/11/2011"

    if docid=="m02690":
        date="24/11/2010"

    if docid=="m01124":
        date="31/5/2005"

    if docid=="m02714":
        date="13/12/2010"

    if docid=="m02691":
        date="29/11/2010"

    if docid=="m02602":
        date="2/8/2010"

    if docid=="m03054":
        date="22/02/2011"

    if docid=="m02748":
        date="24/11/2010"

    if docid=="m03083":
        date="25/6/2012"

    if docid=="m02879":
        date="4/7/2011"

    if docid=="m02687":
        date="10/11/2010"

    if docid=="m02571":
        date="28/6/2010"

    if docid=="m03094":
        date="16/5/2011"

    if docid=="m02873":
        date="27/10/2010"

    if docid=="m03005":
        date="7/2/2012"

    if docid=="m02855":
        date="25/5/2011"

    if docid=="m02652":
        date="20/10/2010"

    if docid=="m02875":
        date="26/4/2010"

    if docid=="m02834":
        date="19/7/2010"

    if docid=="m02695":
        date="20/11/2010"

    if docid=="m02692":
        date="29/11/2010"

    if docid=="m02833":
        date="30/11/2010"

    if docid=="m03001":
        date="12/12/2011"
    if docid=="m02619":
        date="1/7/2009"
    if docid=="m03018":
        date="16/2/2010"
    if docid=="m02688":
        date="22/11/2010"
    if docid=="m02831":
        date="26/12/2010"
    if docid=="m02709":
        date="7/12/2010"
    if docid=="m02897":
        date="21/6/2011"
    if docid=="m02042":
        date="6/11/2007"
    if docid=="m02849":
        date="8/3/2011"
    if docid=="m03097":
        date="2/7/2012"
    if docid=="m02990":
        date="3/1/2012"
    if docid=="m02744":
        date="20/12/2010"
    if docid=="m03093":
        date="12/1/2011"
    if docid=="m02902":
        date="15/2/2011"
    if docid=="m02693":
        date="9/11/2010"
    if docid=="m02725":
        date="24/11/2010"

    if date:
        return date.split("/")
    else:
        return None

def extract_date(docid,line):
    mc=re.search(u"(\d+)\s*(\w+)\s*(\d{4})",line,flags=re.U)
    if mc:

        l=date_by_docid_override(docid)
        if l:
            d,m,y=l
        else:
            d,m,y=mc.group(1),mc.group(2),mc.group(3)

        date=handle_date(d,m,y)

        if date:
            return {'docid':docid, 'date': date}
        else:
            return None

#def main():
#    if os.path.isfile(settings.DATE_TXT_FILE):
#        ls=codecs.open(settings.DATE_TXT_FILE,encoding='utf-8').readlines()
#    else:
#        print "Couldn't find %s" % settings.DATE_TXT_FILE
#        sys.exit(1)
#
#    ls=select_lines(ls)
#
#    for line in ls:
#        docid=line.split()[0]
#        d=process_line(docid,line)
#        if d:
#            print "%s: %s" % (d['docid'],d['date'])
#    pass
#
#if __name__ == '__main__':
#    main()