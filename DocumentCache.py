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

import subprocess

import sys,os,re,json,codecs
import logging
import urllib

import settings
import utils

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s %(name)-4s %(levelname)-8s %(message)s',
    datefmt='%m-%d %H:%M:%S',
    #		    filename='/tmp/myapp.log',
    #		    filemode='w'
)
logger = logging.getLogger("mmm-scrape")

class DocumentCache(object):
    def __init__(self, datadict):
        self.datadict = datadict
        self.populateLocalCache()

    def populateLocalCache(self):
        """
        iterates over document entries in self.datadict, retrievs documents
        from net if not already cached, extracts text from documents if not already cached
        """
        logger.info("Populating local document cache, retrieving from net as needed")
        for (k, d) in sorted(self.datadict.iteritems()):
            basename = utils.get_base_name(d['url'])

            fullpath = os.path.join(settings.DATADIR, basename)
            fulltxtpath = os.path.join(settings.DATADIR, basename.split('.')[0] + ".txt")
            if not os.path.exists(fulltxtpath) and not os.path.exists(fullpath):
                logger.info("Retrieving %s into %s" % (d['url'], settings.DATADIR))
                with open(fullpath, "wb") as f:
                    f.write(urllib.urlopen(d['url']).read())
                    pass

            if not os.path.exists(fulltxtpath):
                cmd = "pdftotext -f 1 -l 5 %s -" % fullpath
                logger.info("converting %s to text" % fullpath)

                p = subprocess.Popen(cmd.strip().split(' '), stdout=subprocess.PIPE)
                (contents, errf) = p.communicate()
                with codecs.open(fulltxtpath, "wb", encoding='utf-8') as f:
                    f.write(contents.decode('utf-8'))

                if settings.DELETE_PDF_AFTER_EXTRACTION:
                    os.unlink(fullpath)
    def sanitize_lines(self,lines):
        lines = [x.decode('utf-8') for x in lines]
        lines = [re.sub(u"['`\"]", "", x) for x in lines]
        lines = [re.sub(u"[^א-ת\d]", " ", x) for x in lines]
        lines = [re.sub(u"וו?עד", u"ועד", x) for x in lines]
        lines = [re.sub(u"[לב]ועד", u"ועד", x) for x in lines]
        #	lines = [re.sub(u"\sה",u" ",x ) for x in lines ]
        lines = [re.sub(u"\s+", " ", x) for x in lines]

        return lines

    def getDocumentLines(self, k):
        d = self.datadict[k]
        basename = utils.get_base_name(d['url'])
        fullpath = os.path.join(settings.DATADIR, basename)
        fulltxtpath = os.path.join(settings.DATADIR, basename.split('.')[0] + ".txt")
        logger.debug("Loading cached text for %s from %s" % (fullpath, fulltxtpath))
        with codecs.open(fulltxtpath, encoding='utf-8') as f:
            contents = f.read().encode('utf-8')

        lines = self.sanitize_lines(contents.split("\n"))
        return lines