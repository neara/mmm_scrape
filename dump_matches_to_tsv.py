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
import settings

def data_to_gen(fname):
    s=json.load(codecs.open(fname))

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
        f.write("\xff\xfe") # BOM, the right one I think
        for l in data:
            f.write(("\t".join(map(unicode,l))+"\n").encode('utf-16le'))


def main():
    g=filter_keys(data_to_gen(codecs.open(settings.MATCHESFILE)))
    write_tsv(g,settings.MATCHES_CSV_FILE)

if __name__ == '__main__':
#	cProfile.run('main()','out.prof')
    main()