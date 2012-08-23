#!/usr/bin/env python
# -*- coding: utf-8 -*-
import subprocess

import sys,os,re,json,codecs
import cProfile
import urllib
from scrape_mmm import logger
import settings
import utils

def main():
    pass
    
if __name__=='__main__':
#	cProfile.run('main()','out.prof')
	main()

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