#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os, re, json, codecs
import settings

def data_to_gen(fname):
    s=json.load(fname)

    for x in s:
        yield x


# use a template dict to ensure data alignment
# in case different entries have different sets of keys
# this will output everything aligned according to the keys
# of the given key_list of the first entry given
def filter_keys(g,key_list=None):
    d=g.next()
    if  key_list is None:
        key_list=d.keys()
        yield key_list
    else:
        yield [v for (k,v) in d.iteritems() if k in key_list ]

    for d in g:
        yield [v for (k,v) in d.iteritems() if k in key_list ]

def write_tsv(data,ofname):

    with codecs.open(ofname,"wb") as f:
        f.write("\xff\xfe")
        for l in data:
            f.write(("\t".join(map(unicode,l))+"\n").encode('utf-16le'))


def main():
    g=filter_keys(data_to_gen(codecs.open(settings.MATCHESFILE)))
    write_tsv(g,settings.MATCHES_CSV_FILE)

if __name__ == '__main__':
#	cProfile.run('main()','out.prof')
    main()