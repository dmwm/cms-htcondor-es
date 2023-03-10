#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>
# This script uses the AffiliationManager class to create or update
# the affiliation cache file (an indexed structure of username/login
# and affiliation institution and country from cric data)
# How to run:
#       python affiliation_cache.py
# This will create a file in $AFFILIATION_DIR_LOCATION called .affiliation_dir.json,
# if the file already exists it will overwrite it if and only if it's older
# than one day. You can specify the location and the recreation period using
# the optional parameters:
#       python affiliation_cache.py --output /htcondor_es/aff_dir.json --days 3
# Thiis script can be setup as a daily cronjob and use the parameters to modify
# how often the script is updated.
import argparse
import os
import traceback

from htcondor_es.AffiliationManager import AffiliationManager, AffiliationManagerException


def generate_affiliation_cache(output_file, days=1):
    """
        Update the cache file if older than a given number of days.
    """
    try:
        AffiliationManager(recreate_older_days=days, dir_file=output_file)
    except AffiliationManagerException:
        traceback.print_exc()


if __name__ == "__main__":
    # Create separate logging for AffiliationManager
    _output_file = os.getenv("AFFILIATION_DIR_LOCATION", None)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default=None,
        help="""location of the affiliation cache file. 
        By default, it will use the AFFILIATION_DIR_LOCATION env variable if set or $SPIDER_WORKDIR as default.""",
    )
    parser.add_argument(
        "--days", type=int, default=1, help="How often should the file be updated? (days)",
    )
    args = parser.parse_args()
    if args.output:
        _output_file = args.output

    generate_affiliation_cache(output_file=_output_file, days=args.days)
