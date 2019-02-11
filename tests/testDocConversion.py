#!/usr/bin/python
"""
Script for testing document conversion
"""

import os
import sys
import json
import pickle
import argparse

import htcondor

try:
    import htcondor_es
except ImportError:
    if os.path.exists("src/htcondor_es/__init__.py") and "src" not in sys.path:
        sys.path.append("src")

from htcondor_es.utils import get_schedds
from htcondor_es.convert_to_json import convert_to_json


def process_pickle(filename, args):
    print "...reading job ads from %s" % filename
    dumpfile_name = filename.strip('.pck')
    dumpfile = os.path.join(args.dump_target, '%s.json' % dumpfile_name)
    count = 0
    with open(filename, 'r') as pfile, open(dumpfile, 'w') as dfile:
        try:
            job_ads = pickle.load(pfile)
        except Exception, e:
            print e

        dict_ads = [convert_to_json(job_ad, return_dict=True) for job_ad in job_ads]
        dict_ads = filter(None, dict_ads)
        json.dump(dict_ads, dfile, indent=4, sort_keys=True)
        count = len(dict_ads)

    print "   ...done, converted %d docs, written to %s" % (count, dumpfile)
    return True


def get_ads_from_schedd(schedd_ad, args):
    schedd = htcondor.Schedd(schedd_ad)
    selection = args.selection or 'true'
    history_iter = schedd.history(selection, [], args.n_docs_to_query)

    job_ads = [j for j in history_iter]

    pckfile = os.path.join(args.filename)
    # If the file already exists, append the ads
    if os.path.isfile(pckfile):
        with open(pckfile, 'r') as pfile:
            job_ads.extend(pickle.load(pfile))

    with open(pckfile, 'w') as pfile:
        pickle.dump(job_ads, pfile, pickle.HIGHEST_PROTOCOL)

    print "   ...done, wrote %d docs to %s" % (len(job_ads), pckfile)
    return True


def main(args):
    if not os.path.isfile(args.filename):
        print "...file doesn't exist, querying schedds"
        schedd_ads = get_schedds(args)

        for schedd_ad in schedd_ads[:args.n_schedds_to_query]:
            print "...processing %s" % schedd_ad["Name"]
            get_ads_from_schedd(schedd_ad, args)

    try:
        os.makedirs(args.dump_target)
    except OSError:
        pass # dir exists


    process_pickle(args.filename, args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str,
                        help=("Read ads from this pickle file. "
                              "If it doesn't exist, query schedds and store "
                              "them here, then convert them."))
    parser.add_argument("--n_docs_to_query", default=1000,
                        type=int, dest="n_docs_to_query",
                        help=("Number of documents to test on "
                              "[default: %(default)d]"))
    parser.add_argument("--n_schedds_to_query", default=5,
                        type=int, dest="n_schedds_to_query",
                        help=("Number of schedds to test on "
                              "[default: %(default)d]"))
    parser.add_argument("--schedd_filter", default='',
                        type=str, dest="schedd_filter",
                        help=("Comma separated list of schedd names to process "
                              "[default is to process all]"))
    parser.add_argument("--selection", default='',
                        type=str, dest="selection",
                        help="classad.ExprTree to select ads")
    parser.add_argument("--dump_target", default='.',
                        type=str, dest="dump_target",
                        help=("Dump converted documents here "
                              "[default: %(default)d]"))

    args = parser.parse_args()
    main(args)
