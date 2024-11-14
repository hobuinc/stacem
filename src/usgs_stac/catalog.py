import os

from urllib.parse import urljoin
from functools import partial
from operator import is_not

import pystac

from dask.diagnostics import ProgressBar
from dask import compute
from distributed import get_client, Client

from .metadata_common import logger, read_json
from .collection import MetaCollection


def do_one(col, cat, idx, total):
    logger.info(f"{col.id} ({idx}/{total})")
    links : list[pystac.Link] = compute(*col.set_children(), scheduler='threading')
    col.collection.add_links(filter(partial(is_not, None), links))
    col.collection.set_root(cat)
    col.save_local()

    del col

class MetaCatalog:
    """
    MetaCatalog reads the WESM JSON file at the given url, and creates a list of
    MetaCollections. Dask Bag helps facilitate the mapping of this in parallel.
    """
    def __init__(self, url: str, dst: str, href: str, update=False) -> None:
        self.update = update
        self.url = url

        if str(dst)[:-1] != '/':
            self.dst = str(dst) + '/'
        else:
            self.dst = str(dst)

        self.children = [ ]
        self.catalog = pystac.Catalog(id='WESM Catalog',
            description='Catalog representing WESM metadata and associated'
                ' point cloud files.')
        self.catalog.set_root(self.catalog)
        self.obj: dict = read_json(self.url)
        self.href = href
        self.catalog.set_self_href(urljoin(href, "catalog.json"))

    def save_local(self):
        """
        Go through the local dest folder and add all collections as child links
        into the catalog. These will be referenced from the dest href as opposed
        to the local dest.
        """
        p = os.path.join(self.dst, "catalog.json")



        self.catalog.save_object(True, p)

    def set_children(self):
        """
        Add child STAC Collections to overall STAC Catalog
        """

        meta_collections = [
            MetaCollection(o, self.href, self.dst, self.update)
            for o in self.obj.values()
        ]
        count = len(meta_collections)

        try:
            client = get_client()
        except:
            client = Client()

        futures = []
        for idx, c in enumerate(meta_collections):
            c: MetaCollection
            futures.append(client.submit(do_one, col=c, cat=self.catalog, idx=idx, total=count))

        with ProgressBar():
            client.gather(futures)

    def get_stac(self):
        """
        Return overall STAC Catalog
        """
        return self.catalog