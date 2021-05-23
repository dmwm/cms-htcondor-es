#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Authors: Christian Ariza <christian.ariza AT gmail [DOT] com>, Ceyhun Uzunoglu <cuzunogl AT gmail [DOT] com>

"""
This script uses the AffiliationManager class to create or update
the affiliation cache file (an indexed structure of username/login
and affiliation institution and country from cric data)

This will create a file in the users' home called .affiliation_dir.json or in output file,
if the file already exists it will overwrite it if and only if it's older
than one day or days parameter. You can specify the location and the recreation period using
the optional parameters:

This script can be setup as a daily cronjob and use the parameters to modify
how often the script is updated.

Notes:
    ``AFFILIATION_DIR_LOCATION`` env variable should be provided.

Attributes:
    output_file (string):  environment variable for affiliation json file output.
        Default is ``AFFILIATION_DIR_LOCATION``. It used in k8s deployment to define affiliation file location.
            ``AFFILIATION_DIR_LOCATION`` file is updated once a day by ``spider-cron-affiliation`` CronJob.
            It is stored in persistent volume called ``shared-spider``.
        Usage in k8s:
            - https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/cronjobs/spider-cron-affiliation.yaml
            - https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/cronjobs/spider-cron-queues.yaml
            - https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/deployments/spider-worker.yaml
        Usage in ``convert_to_json`` is to fetch affiliations and enrich ClassAd attributes with them.

Raises:
        AffiliationManagerException: Generic exception of AffiliationManager

Examples:
    $ python affiliation_cache.py --output /htcondor_es/aff_dir.json --days 3
    $ python affiliation_cache.py # k8s_affiliation_cache.sh calls like this, which run in ``spider-cron-affiliation``.

"""
import argparse
import os
import traceback

from htcondor_es.AffiliationManager import (
    AffiliationManager,
    AffiliationManagerException,
)


def generate_affiliation_cache(output_file, days=1):
    """Update the cache file if older than a given number of days.
    Args:
        output_file (string): Output file to write affiliations
        days (int): recreate_older_days parameter for AffiliationManager

    """
    try:
        AffiliationManager(recreate_older_days=days, dir_file=output_file)
        print("Affiliation creation successful.")
    except AffiliationManagerException as _:
        traceback.print_exc()
        print("There was an error creating the affiliation manager")


if __name__ == "__main__":
    output_file = os.getenv(
        "AFFILIATION_DIR_LOCATION",
        AffiliationManager._AffiliationManager__DEFAULT_DIR_PATH,
    )
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        help="""location of the affiliation cache file
    By default, it will use the AFFILIATION_DIR_LOCATION env variable if set
    or the users' home as default.""",
        default=None,
    )
    parser.add_argument(
        "--days",
        help="How often should the file be updated? (days)",
        type=int,
        default=1,
    )
    args = parser.parse_args()
    if args.output:
        output_file = args.output

    generate_affiliation_cache(output_file, args.days)
