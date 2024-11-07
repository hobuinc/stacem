import os

import json

from urllib.parse import urljoin
from urllib.request import urlopen
from urllib.error import HTTPError
from typing import Union

import subprocess
import pystac
import pyproj
import shapely.wkt
import shapely.ops
import shapely

from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.pointcloud import PointcloudExtension, Schema

from .metadata_common import logger, WesmMetadata, session


class MetaItem:
    """
    MetaItem will run PDAL very coursely over this pointcloud, and create a STAC item
    from it. It will use the sidecar file found in the metadata directory to fill in
    any gaps in information.
    """

    def __init__(self, pc_path: str, meta_path: str, dst: str, href: str,
            wesm_meta: WesmMetadata, update=False):
        self.update = update
        self.pc_path = pc_path
        self.meta_path = meta_path
        self.meta = wesm_meta
        self.parent = self.meta.FESMProjectID
        self.id = os.path.splitext(os.path.basename(pc_path))[0]
        self.dst = os.path.join(dst, self.parent, self.id, f"{self.id}.json")
        parent_href = urljoin(href, os.path.join(self.parent, "collection.json"))
        self.parent_link = pystac.Link(rel='collection', target=parent_href,
            media_type='application/json')
        self.href = urljoin(href, os.path.join(self.parent, self.id, f"{self.id}.json"))

        self.link = pystac.Link(rel='item', target=self.href, media_type='application/json', title=self.id)

        self.errors: list[str] = []
        self.stats = None
        self.item = None
        self.etag = None

    def info(self):
        cargs = ['pdal','info','--metadata', '--schema', str(self.pc_path)]
        p = subprocess.Popen(cargs, stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    encoding='utf8')
        ret = p.communicate()
        if p.returncode != 0:
            error = ret[1]
            return {"args":cargs, "error": error}
        self.stats = json.loads(ret[0])
        return self.stats

    def process(self):
        try:
            self.get_stac()
            self.save_local()
            return self.link
        except Exception as e:
            logger.error(f"{self.id} failed with error {e.args}")
            return None

    def save_local(self):
        if self.item is not None:
            self.item.save_object(True, self.dst)

    def match_etag(self) -> bool:
        # determine if stac info needs to be re-run based on if the etag
        # matches the previous one used.

        try:
            #### Try getting the etag in a previous version of the Item
            try:
                # try looking at self.dst
                local_item = json.load(open(self.dst))
                prev_etag= local_item['properties']['etag']
            except:
                # try looking at self.href
                try:
                    prev_item = pystac.Item.from_file(self.href)
                    prev_etag = prev_item.properties['etag']
                except:
                    prev_etag = None

            try:
                headers = session.head(self.pc_path).headers
                self.etag = headers['ETag']
                if '"' in self.etag:
                    self.etag= self.etag.strip('"')
            except HTTPError:
                # if pc_path fails, default to previous item
                self.etag = prev_etag
            except:
                # if etag fails, may still be a new item so rerun it
                self.etag = None

            if self.etag is None:
                return False
            elif prev_etag is not None and prev_etag == self.etag:
                return True
            else:
                return False

        except Exception as e:
            self.etag = None
            self.errors.append[f"A valid ETag wasn't found at location {self.pc_path}"]
            return False

    def get_properties(self):
        properties = {
            "start_datetime": self.meta.collect_start,
            "end_datetime": self.meta.collect_end,
            "etag": self.etag,
            "seamless_category": self.meta.seamless_category,
            "seamless_reason": self.meta.seamless_reason,
            "onemeter_category": self.meta.onemeter_category,
            "onemeter_reason": self.meta.onemeter_reason,
            "lpc_category": self.meta.lpc_category,
            "lpc_reason": self.meta.lpc_reason,
            "ql": self.meta.ql
        }
        if self.errors:
            properties["errors"] = self.errors
        return properties

    def add_assets(self, item):
        # add data and metadata asset
        if self.pc_path:
            item.add_asset(
                "data",
                pystac.Asset(
                    title="LAS data",
                    href=self.pc_path,
                    roles=["data"],
                    media_type="application/vnd.laszip",
                ),
            )
        if self.meta_path:
            item.add_asset(
                "metadata",
                pystac.Asset(
                    title='Metadata',
                    href=self.meta_path,
                    roles=["metadata"],
                    media_type="application/xml"))
        return item

    def add_extensions(self, item):
        item.stac_extensions.append(
            "https://stac-extensions.github.io/file/v2.1.0/schema.json"
        )
        item.set_self_href(self.href)
        item.add_link(self.parent_link)
        item.collection_id = self.parent
        return item


    def from_metadata(self):
        #item failed to get pdal info, now fill in info purely from metadata
        src_crs = pyproj.CRS.from_user_input(self.meta.horiz_crs)
        dst_crs = pyproj.CRS.from_epsg(4326)
        trn = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)

        meta_bbox = self.meta.bbox
        minx, miny, maxx, maxy = meta_bbox

        left, bottom, right, top = trn.transform_bounds(
            minx, miny, maxx, maxy)
        bbox = [left, bottom, right, top]
        shape = shapely.geometry.box(minx, miny, maxx, maxy)
        geometry = shapely.geometry.mapping(
            shapely.ops.transform(trn.transform, shape))

        properties = self.get_properties()

        item = pystac.Item(
            id=self.id,
            geometry=geometry,
            bbox=bbox,
            datetime=None,
            properties=properties,
        )

        # add projection extension
        projection: ProjectionExtension = ProjectionExtension.ext(item,
            add_if_missing=True)
        projection.epsg = None
        projection.projjson = src_crs.to_json_dict()
        projection.geometry = geometry
        projection.bbox = [minx, miny, maxx, maxy]
        ##########################

        item = self.add_assets(item)

        # add file extensions
        self.add_extensions(item)
        self.item = item

        return item

    def get_stac(self) -> Union[pystac.Item, None]:
        # Determine if the laz file needs to be reprocessed
        try:
            item = pystac.Item.from_file(self.dst)
            self.item = item
            return item
        except Exception as e:
            try:
                item = pystac.Item.from_file(self.href)
                self.item = item
                return item
            except Exception as e:
                self.errors.append(e.args[0])
                pass

        matched = self.match_etag()
        if matched:
            logger.debug(f"No update to {self.id}. Skipping.")
            self.item = pystac.Item.from_file(self.dst)
            return self.item

        # run pdal info over laz data
        # if this returns an obj with "error" key, it has failed
        pdal_metadata = self.info()
        if "error" in pdal_metadata:
            self.errors.append(json.dumps(pdal_metadata))
            return self.from_metadata()

        # Set up the projection transformation
        # transform data to 4326 for STAC, use source crs for projection extension
        src_crs_str = pdal_metadata['metadata']["spatialreference"]
        if src_crs_str:
            src_crs = pyproj.CRS.from_user_input(src_crs_str)
        else:
            # some data referenced by WESM doesn't have SRS info in it
            src_crs = pyproj.CRS.from_user_input(self.meta.horiz_crs)
        dst_crs = pyproj.CRS.from_epsg(4326)
        trn = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)

        # Collect spatial extent information
        minx = pdal_metadata['metadata']["minx"]
        maxx = pdal_metadata['metadata']["maxx"]
        miny = pdal_metadata['metadata']["miny"]
        maxy = pdal_metadata['metadata']["maxy"]
        minz = pdal_metadata['metadata']["minz"]
        maxz = pdal_metadata['metadata']["maxz"]
        left, bottom, right, top = trn.transform_bounds(
            minx, miny, maxx, maxy)
        bbox = [left, bottom, minz, right, top, maxz]
        shape = shapely.geometry.box(minx, miny, maxx, maxy)
        geometry = shapely.geometry.mapping(
            shapely.ops.transform(trn.transform, shape))

        # Additional metadata derived from wesm json
        properties = self.get_properties()

        item = pystac.Item(
            id=self.id,
            collection=self.parent,
            geometry=geometry,
            bbox=bbox,
            datetime=None,
            properties=properties,
        )

        #add pointcloud extension
        pointcloud: PointcloudExtension = PointcloudExtension.ext(item,
            add_if_missing=True)
        pointcloud.type = "lidar"
        pointcloud.schemas = [Schema(v) for v in
            pdal_metadata["schema"]["dimensions"]]
        pointcloud.count = pdal_metadata["metadata"]["count"]
        pointcloud.encoding = "application/vnd.laszip"
        #########################

        # add projection extension
        projection: ProjectionExtension = ProjectionExtension.ext(item,
            add_if_missing=True)
        projection.epsg = None
        projection.projjson = src_crs.to_json_dict()
        projection.geometry = geometry
        projection.bbox = [minx, miny, minz, maxx, maxy, maxz]
        ##########################

        # add data and metadata asset
        item.add_asset(
            "data",
            pystac.Asset(
                title="LAS data",
                href=self.pc_path,
                roles=["data"],
                media_type="application/vnd.laszip",
            ),
        )
        item.add_asset(
            "metadata",
            pystac.Asset(
                title='Metadata',
                href=self.meta_path,
                roles=["metadata"],
                media_type="application/xml"))

        # add file extensions
        item.stac_extensions.append(
            "https://stac-extensions.github.io/file/v2.1.0/schema.json"
        )
        item.set_self_href(self.href)
        item.add_link(self.parent_link)
        item.collection_id = self.parent


        self.item = item
        return item
