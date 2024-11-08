import boto3
from dask.distributed import Client

from .catalog import MetaCatalog
from .metadata_common import logger

WESM_URL = 'https://apps.nationalmap.gov/lidar-explorer/lidar_ndx.json'

def usgs_stac():
    client = Client(n_workers=10, threads_per_worker=3)

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--wesm_url", type=str, help="Url for WESM JSON",
            default=WESM_URL)
    parser.add_argument("--local_dst", type=str, help="Destination directory",
            default='./wesm_stac/')
    parser.add_argument("--s3_dst", type=str,
        help="HTTPS S3 path to base STAC location.",
        default="https://s3-us-west-2.amazonaws.com/usgs-lidar-public/wesm_stac/")
    parser.add_argument("--update", type=bool, help="Update flag. If set, will "
            "update JSON objects that already exist with new values.", default=True)


    args = parser.parse_args()

    logger.info(f"WESM url: {args.wesm_url}")
    logger.info(f"Local dest: {args.local_dst}")
    logger.info(f"Remote dest: {args.s3_dst}")
    logger.info(f"Updating: {args.update}")
    logger.info(f"Dask Dashboard Link: {client.cluster.dashboard_link}")

    m = MetaCatalog(args.wesm_url, args.local_dst, args.s3_dst, args.update)
    m.set_children()
    m.save_local()

if __name__ == '__main__':
    usgs_stac()