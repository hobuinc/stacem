import os

from urllib.parse import urljoin

import boto3
import pystac
import json
import dask

from dask.diagnostics import ProgressBar

from .metadata_common import logger, DEFAULT_LOCAL_DST

s3 = boto3.client('s3')

@dask.delayed
def process_one_s3(bucket, path):
    obj = s3.get_object(Bucket=bucket, Key=path)
    col_dict = json.loads(obj['Body'].read())
    c = pystac.Collection.from_dict(col_dict)
    return c

def s3_collections(bucket, prefix):
    count = 0
    pager = s3.get_paginator('list_objects_v2')
    it = pager.paginate(Bucket=bucket, Prefix=prefix)
    objs = it.search("Contents[?contains(Key, `collection.json`)][]")
    f_list = []

    for i in objs:
        path = i['Key']
        print(f"Found collection {path}, {count}")
        f_list.append(process_one_s3(bucket, path))
        count = count + 1

    return f_list

def local_collections(dst):
    ## Find the collections available in this dst directory
    cols_list = []
    _, dirs, _ = next(os.walk(dst))
    for d in dirs:
        newp = os.path.join(dst, d)
        _, _, files = next(os.walk(newp))
        if 'collection.json' in files:
            cols_list.append(pystac.Collection.from_file(
                os.path.join(newp, 'collection.json')))

    return cols_list

def finalize():
    """
    This will create a STAC Catalog that references each of the child Collections
    present in the source directory that is passed in. By default this will look
    for the local directory, but if an s3 bucket and path is passed in, then it
    will use that instead.
    """
    import argparse
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--local_path", type=str, help="Finalize will use this "
        "argument for location by default if no s3 bucket is supplied.",
        default=DEFAULT_LOCAL_DST)
    parser.add_argument("--s3_bucket", type=str, help="S3 destination bucket. "
        "Finalize will use this argument if it's supplied.")
    parser.add_argument("--s3_path", type=str,
        help="S3 path within destination bucket", default='/')
    parser.add_argument("--out_dst", type=str, help="Catalog destination path.",
        default="./catalog.json")

    args = parser.parse_args()

    logger.info(f"Local dest: {args.local_path}")
    logger.info(f"S3 Bucket: {args.s3_bucket}")
    logger.info(f"S3 Path: {args.s3_path}")
    logger.info(f"Output path: {args.out_dst}")

    try:
        cat = pystac.Catalog(id='WESM Catalog',
            description='Catalog representing WESM metadata and associated'
                ' point cloud files.')
        if args.s3_bucket is not None:
            future_cols = s3_collections(args.s3_bucket, args.s3_path)

            with ProgressBar():
                cols = dask.compute(*future_cols)

        else:
            if not os.path.isdir(args.local_path):
                raise ValueError("Invalid '--local_path'.")
            cols = local_collections(args.local_path)

        if not cols:
            raise Exception("No collections found.")

        # find catalog path from first collection in array
        c1 = cols[0]
        c1:pystac.Collection
        catalog_path = c1.get_root_link().href
        cat.add_children(cols)
        cat.set_self_href(catalog_path)
        cat.save_object(args.out_dst)

        logger.info("Done! Catalog saved to {args.out_dst}.")

    except Exception as e:
        logger.error(e.args)
        parser.print_help()
        exit(1)

if __name__ == '__main__':
    finalize()