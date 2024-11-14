import os
from dask.distributed import Client

from .catalog import MetaCatalog
from .metadata_common import (
    logger,
    DEFAULT_LOCAL_DST,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_PATH,
    DEFAULT_WESM_URL
)

def create():
    """
    Create command. Creates STAC from scratch or from previously created
    STAC structure. If it has already been made before, then this command
    will update Items to reflect any new information that has been added.
    """
    client = Client()

    import argparse
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--wesm_url", type=str, help="Url for WESM JSON",
            default=DEFAULT_WESM_URL)
    parser.add_argument("--local_dst", type=str, help="Destination directory",
            default=DEFAULT_LOCAL_DST)
    parser.add_argument("--s3_bucket", type=str, help="S3 destination bucket",
        default=DEFAULT_S3_BUCKET)
    parser.add_argument("--s3_path", type=str, help="S3 path within destination bucket",
        default=DEFAULT_S3_PATH)
    parser.add_argument("--update", type=bool, help="Update flag. If set, will "
            "update JSON objects that already exist with new values.", default=True)

    try:
        args = parser.parse_args()
        s3_dst = f"https://{args.s3_bucket}.s3.amazonaws.com/{args.s3_path}"

        logger.info(f"WESM url: {args.wesm_url}")
        logger.info(f"Local dest: {args.local_dst}")
        logger.info(f"Remote dest: {s3_dst}")
        logger.info(f"Updating: {args.update}")

        m = MetaCatalog(args.wesm_url, args.local_dst, s3_dst, args.update)
        logger.info(f"Dask Dashboard Link: {client.cluster.dashboard_link}")
        m.set_children()
    except Exception as e:
        logger.error(e.args)
        parser.print_help()
        exit(1)

if __name__ == '__main__':
    create()