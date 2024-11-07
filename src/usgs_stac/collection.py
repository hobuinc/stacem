import os
import json

from datetime import datetime
from urllib.parse import urljoin
from urllib.request import urlopen

import pystac

from dask import delayed

from .metadata_common import PCParser, WesmMetadata, session
from .item import MetaItem

class MetaCollection:
    """
    MetaCollection will read the corresponding JSON section, pull relevant info from
    it, and create a list of MetaItems from the directory of laz files found at the
    pointcloud path found in the JSON.
    """

    def __init__(self, obj: dict[str, any], href: str, dst: str, update=False):
        self.update = update
        self.meta = WesmMetadata(**obj)
        self.id = self.meta.FESMProjectID
        self.root_dst = dst
        self.root_href = href

        self.valid = True
        if self.meta.lpc_link is None:
            self.valid = False

        self.dl_link_txt = urljoin(self.meta.lpc_link, '0_file_download_links.txt')
        self.pc_dir = urljoin(self.meta.lpc_link, 'LAZ/')
        self.sidecar_dir = urljoin(self.meta.lpc_link, 'metadata/')
        self.href = urljoin(href, os.path.join(self.id, "collection.json"))
        self.dst = os.path.join(dst, self.id, f"collection.json")
        extra_fields = {
            "seamless_category": self.meta.seamless_category,
            "seamless_reason": self.meta.seamless_reason,
            "onemeter_category": self.meta.onemeter_category,
            "onemeter_reason": self.meta.onemeter_reason,
            "lpc_category": self.meta.lpc_category,
            "lpc_reason": self.meta.lpc_reason,
            "ql": self.meta.ql
        }

        e = pystac.Extent(
            spatial=pystac.SpatialExtent(bboxes=self.meta.bbox),
            temporal=pystac.TemporalExtent(intervals=[
                datetime.fromisoformat(self.meta.collect_start),
                datetime.fromisoformat(self.meta.collect_end)
            ])
        )
        self.collection = pystac.Collection(
            id=self.id,
            description=f'STAC Collection for USGS Project {self.id} derived'
                ' from WESM JSON.',
            extent=e,
            extra_fields=extra_fields
        )
        if self.meta.metadata_link and self.meta.metadata_link is not None:
            meta_asset = pystac.Asset(href=self.meta.metadata_link, title='metadata',
                description='Metadata', media_type='text/html')
            self.collection.add_asset(key='metadata', asset=meta_asset)
        if self.meta.lpc_link and self.meta.lpc_link is not None:
            lpc_asset = pystac.Asset(href=self.meta.lpc_link, title='pointcloud_links',
                description='Pointcloud Links Page', media_type='text/html')
            self.collection.add_asset(key='pointcloud_links', asset=lpc_asset)
        if self.meta.sourcedem_link and self.meta.sourcedem_link is not None:
            dem_asset = pystac.Asset(href=self.meta.sourcedem_link, title='sourcedem_links',
                description='DEM Raster Links', media_type='text/html')
            self.collection.add_asset(key='sourcedem_links', asset=dem_asset)

        self.collection.set_self_href(self.href)
        self.pc_paths = []
        self.sidecar_paths = []
        self.link = pystac.Link(rel='collection', target=self.href, media_type='application/json')

    def save_local(self) -> None:
        self.collection.save_object(True, self.dst)

    def set_paths(self) -> None:
        # grab pointcloud paths and sidecar paths
        try:
            self.pc_paths = [p.decode('utf-8').rstrip() for p in urlopen(self.dl_link_txt)]
            self.sidecar_paths = [p.replace('.laz', '.xml').replace('LAZ', 'metadata') for p in self.pc_paths]
        except Exception as e:
            try:
                res = session.get(self.pc_dir)

                parser = PCParser()
                parser.feed(res.text)
                self.pc_paths = [urljoin(self.pc_dir, p) for p in parser.messages]

                meta_messages = [m.replace('.laz', '.xml') for m in parser.messages]
                self.sidecar_paths = [
                    urljoin(self.sidecar_dir, m) for m in meta_messages
                ]
            except:
                self.valid = False

    def set_children(self) -> None:
        """
        Add children to the project STAC Collection
        """
        if not self.valid:
            return []

        self.set_paths()

        vars = zip(self.pc_paths, self.sidecar_paths)
        obj_list: list[MetaItem] = [
            MetaItem(p, s, self.root_dst, self.root_href, self.meta, self.update)
            for p, s in vars
        ]

        return [delayed(lambda i: i.process())(item) for item in obj_list]

    def get_stac(self) -> pystac.Collection:
        """
        Return project STAC Collection
        """
        return self.collection

    def __repr__(self):
        return json.dumps(self.meta)