import os
from typing import Any
from pathlib import Path

from dask import compute
from pystac import Catalog, Item
from usgs_stac.catalog import MetaCatalog
from usgs_stac.collection import MetaCollection
from usgs_stac.item import MetaItem
from usgs_stac.metadata_common import get_date, WesmMetadata

def test_metadata(meta_json: dict[str, Any]) -> None:
    m = WesmMetadata(**meta_json)

    assert m.FESMProjectID == meta_json['FESMProjectID']
    assert m.horiz_crs == meta_json['horiz_crs']
    assert m.vert_crs == meta_json['vert_crs']
    assert m.metadata_link == meta_json['metadata_link']
    assert m.collect_start == get_date(meta_json['collect_start'])
    assert m.collect_end == get_date(meta_json['collect_end'])

def test_item(meta_json: dict[str, Any], s3_url: str, dst_dir: Path, item_json):
    m = MetaCollection(meta_json, s3_url, dst_dir)
    assert m.collection.validate()

    m.set_paths()
    pc_path = m.pc_paths[0]
    meta_path = m.sidecar_paths[0]

    mi = MetaItem(pc_path, meta_path, dst_dir, s3_url, m.meta)
    item = mi.get_stac()
    assert item.validate()

    # test that changes are made to the item
    test_item = Item.from_dict(item_json)
    assert test_item.properties != item.properties


    meta_item = mi.from_metadata()
    assert meta_item.validate()

def test_redo(meta_json: dict[str, Any], s3_url: str, dst_dir: Path):
    m = MetaCollection(meta_json, s3_url, str(dst_dir))
    assert m.collection.validate()

    m.set_paths()
    pc_path = m.pc_paths[0]
    meta_path = m.sidecar_paths[0]

    mi = MetaItem(pc_path, meta_path, dst_dir, s3_url, m.meta)

    mi.process()
    assert mi.item.validate()

    # should not run pdal call again
    assert mi.match_etag(mi.item)

    d2 = list(os.walk(dst_dir/"WY_YELLOWSTONENP_1RF_2020"))[0][1] # Find item directories
    assert len(d2) == 1

def test_item_update(meta_json: dict[str, Any], s3_url: str, dst_dir: Path):
    m = MetaCollection(meta_json, s3_url, str(dst_dir))
    assert m.collection.validate()

    m.set_paths()
    pc_path = m.pc_paths[0]
    meta_path = m.sidecar_paths[0]

    mi = MetaItem(pc_path, meta_path, dst_dir, s3_url, m.meta)

    mi.process()
    assert mi.item.validate()

    # should draw the item written previously to the local storage
    # and update the links to be based on this test url
    s3_new_url = 'https://asdf.asdfwesmtests.com/wesm_stac/'
    mi2 = MetaItem(pc_path, meta_path, dst_dir, s3_new_url, m.meta)
    mi2.process()
    assert mi2.item.self_href == mi2.href

def test_full_loop(wesm_url: dict[str, Any], dst_dir, s3_url):

    m = MetaCatalog(wesm_url, dst_dir, s3_url)
    assert m.url == wesm_url
    assert m.children == []
    # reset and make a more reasonable size for testing
    # Colorado data set currently fails, added to test to make sure
    # that even when it does we still write out the other 2
    obj = {
        "WA_PSLC_2000": m.obj["WA_PSLC_2000"],
        "WY_YELLOWSTONENP_1RF_2020": m.obj["WY_YELLOWSTONENP_1RF_2020"],
    }
    m.obj = { }
    for i, kv in enumerate(obj.items()):
        if i >= 3:
            break
        m.obj[kv[0]] = kv[1]

    m.set_children()
    m.save_local()
    assert m.catalog.validate()
    d1 = list(os.walk(dst_dir/"WA_PSLC_2000"))[0][1] # Find item directories
    assert len(d1) == 49

    d2 = list(os.walk(dst_dir/"WY_YELLOWSTONENP_1RF_2020"))[0][1]
    assert len(d2) == 1

    #check that catalog has links to collections
    cols: list[Catalog] = list(m.catalog.links)
    assert len(cols) == 4