import os
import argparse
import traceback
from htcondor_es.AffiliationManager import (
    AffiliationManager,
    AffiliationManagerException,
)


def generate_affiliation_cache(output_file, days=1):
    """
        Update the cache file if older than a given number of days.
    """
    try:

        AffiliationManager(
            recreate_older_days=days, dir_file=output_file,
        )
    except AffiliationManagerException as e:
        traceback.print_exc()
        print("There was an error creating the affiliation manager")


if __name__ == "main":
    output_file = os.getenv(
        "AFFILIATION_DIR_LOCATION",
        AffiliationManager._AffiliationManager__DEFAULT_DIR_PATH,
    )
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        help="""location of the affiliation cache file
    By default, it will use the AFFILIATION_DIR_LOCATION env variable if set
    or the users home as default.""",
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
