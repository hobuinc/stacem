import sys
import logging
import requests
import io
import json

from typing import Any, Tuple
from dataclasses import dataclass
from datetime import datetime
from urllib.request import urlopen

from html.parser import HTMLParser
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger('wesm_stac')
ch = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
if logger.level == 0:
    logger.setLevel(logging.INFO)

session = requests.Session()
retries = Retry(total=5,
                status_forcelist=[ 500, 502, 503, 504 ])
session.mount('https://rockyweb.usgs.gov/', HTTPAdapter(max_retries=retries))

s3_session = requests.Session()

def read_json(filename, stdin=False):
    if stdin:
        buffer = sys.stdin.buffer
    elif 'https://' in filename:
        buffer = urlopen(filename)
    else:
        buffer = open(filename, 'rb')

    stream = io.TextIOWrapper(buffer, encoding='utf-8')
    pipe = stream.read()
    pipe = json.loads(pipe)
    return pipe


@dataclass
class WesmMetadata:
    FESMProjectID: str
    Entwined: bool
    EntwinePath: str
    LAZinCloud: bool
    FolderName: str
    workunit: str
    workunit_id: float
    project: str
    project_id: float
    collect_start: str
    collect_end: str
    ql: str
    spec: str
    p_method: str
    dem_gsd_meters: float
    horiz_crs: str
    vert_crs: str
    geoid: str
    lpc_pub_date: datetime
    lpc_category: str
    lpc_update: str
    lpc_reason: str
    sourcedem_pub_date: str
    sourcedem_update: str
    sourcedem_category: str
    sourcedem_reason: str
    onemeter_category: str
    onemeter_reason: str
    seamless_category: str
    seamless_reason: str
    lpc_link: str
    sourcedem_link: str
    metadata_link: str
    bbox: Tuple[float, float, float, float]

    def __post_init__(self):
        self.collect_end = get_date(self.collect_end)
        self.collect_start = get_date(self.collect_start)

        if self.lpc_link is None or not self.lpc_link:
            pass
        elif self.lpc_link[:-1] != '/':
            self.lpc_link = self.lpc_link + '/'

        if self.bbox:
            str_box = self.bbox.strip(' ').split(',')
            self.bbox = [float(v) for v in str_box]

# date collected
# WESM Docs say it should be YYYY-MM-DD (isoformat), but I'm also seeing
# YYYY/MM/DD, which is not isoformat so we're covering both.
def get_date(d:str) -> Any:
    try:
        dt = datetime.isoformat(d)
    except:
        try:
            dt = datetime.strptime(d, '%Y/%m/%d')
        except Exception as e:
            raise ValueError(f'Invalid datetime ({d}).', e)
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

class PCParser(HTMLParser):
    """
    Parser HTML returned from rockyweb endpoints, finding laz files associated
    with a specific project. These laz files also share names (minus suffix) with
    the metadata files, which are located in the metadata directory up a level.
    """
    def __init__(self):
        HTMLParser.__init__(self)
        self.messages = []

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        attrs_json = dict(attrs)
        if tag == 'a':
            for k,v in attrs_json.items():
                if k == 'href' and '.laz' in v:
                    self.messages.append(v)