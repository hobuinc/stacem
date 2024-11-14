import pytest
import dask
import logging

from usgs_stac.catalog import MetaCatalog
from usgs_stac.metadata_common import logger
from distributed import Client

@pytest.fixture(autouse=True)
def dask_conf():
    client = Client(processes=True, threads_per_worker=1, n_workers=1)
    yield client

    client.close()

@pytest.fixture(autouse=True)
def log():
    logger.setLevel(logging.DEBUG)

@pytest.fixture(autouse=True)
def dst_dir(tmp_path_factory: pytest.TempPathFactory):
    yield tmp_path_factory.mktemp("test_dst")

@pytest.fixture(autouse=True)
def s3_url():
    yield 'https://hobu-lidar-test.s3.us-east-1.amazonaws.com/wesm_stac/'

@pytest.fixture
def wesm_url():
    yield 'https://apps.nationalmap.gov/lidar-explorer/lidar_ndx.json'

@pytest.fixture
def catalog(wesm_url: str, dst_dir):
    yield MetaCatalog(wesm_url)

@pytest.fixture
def meta_json():
    yield {
        "FESMProjectID": "WY_YELLOWSTONENP_1RF_2020",
        "Entwined": "True",
        "EntwinePath": "WY_YellowstoneNP_1RF_2020",
        "LAZinCloud": "True",
        "FolderName": "WY_YellowstoneNP_2020_D20/WY_YellowstoneNP_1RF_2020",
        "workunit": "WY_YellowstoneNP_1RF_2020",
        "workunit_id": 225074,
        "project": "WY_YellowstoneNP_2020_D20",
        "project_id": 196958,
        "collect_start": "2020/09/21",
        "collect_end": "2021/09/13",
        "ql": "QL 2",
        "spec": "USGS Lidar Base Specification 2.1",
        "p_method": "linear-mode lidar",
        "dem_gsd_meters": 1.0,
        "horiz_crs": "6341",
        "vert_crs": "5703",
        "geoid": "GEOID18",
        "lpc_pub_date": "2022/12/14",
        "lpc_update": None,
        "lpc_category": "Meets",
        "lpc_reason": "Meets 3DEP LPC requirements",
        "sourcedem_pub_date": "2022/12/14",
        "sourcedem_update": None,
        "sourcedem_category": "Meets",
        "sourcedem_reason": "Meets 3DEP source DEM requirements",
        "onemeter_category": "Meets",
        "onemeter_reason": "Meets 3DEP 1-m DEM requirements",
        "seamless_category": "Meets",
        "seamless_reason": "Meets 3DEP seamless DEM requirements",
        "lpc_link": "https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/WY_YellowstoneNP_2020_D20/WY_YellowstoneNP_1RF_2020",
        "sourcedem_link": "http://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Elevation/OPR/Projects/WY_YellowstoneNP_2020_D20/WY_YellowstoneNP_1RF_2020",
        "metadata_link": "http://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Elevation/metadata/WY_YellowstoneNP_2020_D20/WY_YellowstoneNP_1RF_2020",
        "bbox": "-110.4908935097, 45.1792846597, -110.4780893877, 45.1883487461"
    }

@pytest.fixture
def bad_meta():
   return {
        "FESMProjectID": "ARRA_GA_OKEFENOKEE_2010",
        "Entwined": "False",
        "EntwinePath": "",
        "LAZinCloud": "True",
        "FolderName": "legacy/ARRA_GA_OKEFENOKEE_2010",
        "workunit": "ARRA_GA_OKEFENOKEE_2010",
        "workunit_id": -1101,
        "project": "ARRA_GA_OKEFENOKEE_2010_Legacy_Data",
        "project_id": -11010,
        "collect_start": "2010/03/12",
        "collect_end": "2010/04/11",
        "ql": "Other",
        "spec": "Other",
        "p_method": "linear-mode lidar",
        "dem_gsd_meters": 3.0,
        "horiz_crs": "3747",
        "vert_crs": "5703",
        "geoid": "Unknown",
        "lpc_pub_date": "2012/06/28",
        "lpc_update": None,
        "lpc_category": "Does not meet",
        "lpc_reason": "LPC predates v.1.0 or draft LBS",
        "sourcedem_pub_date": None,
        "sourcedem_update": None,
        "sourcedem_category": "Does not meet",
        "sourcedem_reason": "LPC does not meet",
        "onemeter_category": "Does not meet",
        "onemeter_reason": "LPC does not meet",
        "seamless_category": "Does not meet",
        "seamless_reason": "LPC does not meet",
        "lpc_link": "https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/legacy/ARRA_GA_OKEFENOKEE_2010",
        "sourcedem_link": None,
        "metadata_link": "http://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Elevation/metadata/legacy/ARRA-GA_OKEFENOKEE_2010",
        "bbox": "-82.7073, 30.3445, -81.8841, 31.4777"
    }

@pytest.fixture
def item_json():
    # intentionally missing keys so we can demonstrate updating the json in tests
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "USGS_LPC_WY_YellowstoneNP_2020_D20_12TWR400030",
        "properties": {
            "start_datetime": "2020-09-21T00:00:00Z",
            "end_datetime": "2021-09-13T00:00:00Z",
            "etag": "1d339b9-5efbd61df8a2e",
            "onemeter_category": "Meets",
            "onemeter_reason": "Meets 3DEP 1-m DEM requirements",
            "lpc_category": "Meets",
            "lpc_reason": "Meets 3DEP LPC requirements",
            "ql": "QL 2",
            "pc:type": "lidar",
            "pc:schemas": [
                {
                    "name": "X",
                    "size": 8,
                    "type": "floating"
                },
                {
                    "name": "Y",
                    "size": 8,
                    "type": "floating"
                },
                {
                    "name": "Z",
                    "size": 8,
                    "type": "floating"
                },
                {
                    "name": "Intensity",
                    "size": 2,
                    "type": "unsigned"
                },
                {
                    "name": "ReturnNumber",
                    "size": 1,
                    "type": "unsigned"
                },
                {
                    "name": "NumberOfReturns",
                    "size": 1,
                    "type": "unsigned"
                },
                {
                    "name": "ScanDirectionFlag",
                    "size": 1,
                    "type": "unsigned"
                },
                {
                    "name": "EdgeOfFlightLine",
                    "size": 1,
                    "type": "unsigned"
                },
                {
                    "name": "Classification",
                    "size": 1,
                    "type": "unsigned"
                },
                {
                    "name": "Synthetic",
                    "size": 1,
                    "type": "unsigned"
                },
                {
                    "name": "KeyPoint",
                    "size": 1,
                    "type": "unsigned"
                },
                {
                    "name": "Withheld",
                    "size": 1,
                    "type": "unsigned"
                },
                {
                    "name": "Overlap",
                    "size": 1,
                    "type": "unsigned"
                },
                {
                    "name": "ScanAngleRank",
                    "size": 4,
                    "type": "floating"
                },
                {
                    "name": "UserData",
                    "size": 1,
                    "type": "unsigned"
                },
                {
                    "name": "PointSourceId",
                    "size": 2,
                    "type": "unsigned"
                },
                {
                    "name": "GpsTime",
                    "size": 8,
                    "type": "floating"
                },
                {
                    "name": "ScanChannel",
                    "size": 1,
                    "type": "unsigned"
                }
            ],
            "pc:count": 5306053,
            "pc:encoding": "application/vnd.laszip",
            "proj:epsg": None,
            "proj:projjson": {
                "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                "type": "CompoundCRS",
                "name": "NAD83(2011) / UTM zone 12N + NAVD88 height - Geoid18 (metre)",
                "components": [
                    {
                        "type": "ProjectedCRS",
                        "name": "NAD83(2011) / UTM zone 12N",
                        "base_crs": {
                            "name": "NAD83(2011)",
                            "datum": {
                                "type": "GeodeticReferenceFrame",
                                "name": "NAD83 (National Spatial Reference System 2011)",
                                "ellipsoid": {
                                    "name": "GRS 1980",
                                    "semi_major_axis": 6378137,
                                    "inverse_flattening": 298.257222101
                                }
                            },
                            "coordinate_system": {
                                "subtype": "ellipsoidal",
                                "axis": [
                                    {
                                        "name": "Geodetic latitude",
                                        "abbreviation": "Lat",
                                        "direction": "north",
                                        "unit": "degree"
                                    },
                                    {
                                        "name": "Geodetic longitude",
                                        "abbreviation": "Lon",
                                        "direction": "east",
                                        "unit": "degree"
                                    }
                                ]
                            },
                            "id": {
                                "authority": "EPSG",
                                "code": 6318
                            }
                        },
                        "conversion": {
                            "name": "UTM zone 12N",
                            "method": {
                                "name": "Transverse Mercator",
                                "id": {
                                    "authority": "EPSG",
                                    "code": 9807
                                }
                            },
                            "parameters": [
                                {
                                    "name": "Latitude of natural origin",
                                    "value": 0,
                                    "unit": "degree",
                                    "id": {
                                        "authority": "EPSG",
                                        "code": 8801
                                    }
                                },
                                {
                                    "name": "Longitude of natural origin",
                                    "value": -111,
                                    "unit": "degree",
                                    "id": {
                                        "authority": "EPSG",
                                        "code": 8802
                                    }
                                },
                                {
                                    "name": "Scale factor at natural origin",
                                    "value": 0.9996,
                                    "unit": "unity",
                                    "id": {
                                        "authority": "EPSG",
                                        "code": 8805
                                    }
                                },
                                {
                                    "name": "False easting",
                                    "value": 500000,
                                    "unit": "metre",
                                    "id": {
                                        "authority": "EPSG",
                                        "code": 8806
                                    }
                                },
                                {
                                    "name": "False northing",
                                    "value": 0,
                                    "unit": "metre",
                                    "id": {
                                        "authority": "EPSG",
                                        "code": 8807
                                    }
                                }
                            ]
                        },
                        "coordinate_system": {
                            "subtype": "Cartesian",
                            "axis": [
                                {
                                    "name": "Easting",
                                    "abbreviation": "",
                                    "direction": "east",
                                    "unit": "metre"
                                },
                                {
                                    "name": "Northing",
                                    "abbreviation": "",
                                    "direction": "north",
                                    "unit": "metre"
                                }
                            ]
                        },
                        "id": {
                            "authority": "EPSG",
                            "code": 6341
                        }
                    },
                    {
                        "type": "VerticalCRS",
                        "name": "NAVD88 height",
                        "datum": {
                            "type": "VerticalReferenceFrame",
                            "name": "North American Vertical Datum 1988"
                        },
                        "coordinate_system": {
                            "subtype": "vertical",
                            "axis": [
                                {
                                    "name": "Gravity-related height",
                                    "abbreviation": "",
                                    "direction": "up",
                                    "unit": "metre"
                                }
                            ]
                        },
                        "geoid_model": {
                            "name": "GEOID18"
                        },
                        "id": {
                            "authority": "EPSG",
                            "code": 5703
                        }
                    }
                ]
            },
            "proj:geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            -110.4781664978673,
                            45.179290023619394
                        ],
                        [
                            -110.47808425669801,
                            45.188291195766254
                        ],
                        [
                            -110.49081327414119,
                            45.1883486560674
                        ],
                        [
                            -110.49089350974215,
                            45.179347465990176
                        ],
                        [
                            -110.4781664978673,
                            45.179290023619394
                        ]
                    ]
                ]
            },
            "proj:bbox": [
                540000,
                5003000,
                2852.31,
                540999.99,
                5003999.99,
                3231.76
            ],
            "datetime": None
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [
                        -110.4781664978673,
                        45.179290023619394
                    ],
                    [
                        -110.47808425669801,
                        45.188291195766254
                    ],
                    [
                        -110.49081327414119,
                        45.1883486560674
                    ],
                    [
                        -110.49089350974215,
                        45.179347465990176
                    ],
                    [
                        -110.4781664978673,
                        45.179290023619394
                    ]
                ]
            ]
        },
        "links": [
            {
                "rel": "self",
                "href": "https://hobu-lidar-test.s3.us-east-1.amazonaws.com/wesm_stac/WY_YELLOWSTONENP_1RF_2020/USGS_LPC_WY_YellowstoneNP_2020_D20_12TWR400030/USGS_LPC_WY_YellowstoneNP_2020_D20_12TWR400030.json",
                "type": "application/json"
            },
            {
                "rel": "collection",
                "href": "https://hobu-lidar-test.s3.us-east-1.amazonaws.com/wesm_stac/WY_YELLOWSTONENP_1RF_2020/collection.json",
                "type": "application/json"
            }
        ],
        "assets": {
            "data": {
                "href": "https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/WY_YellowstoneNP_2020_D20/WY_YellowstoneNP_1RF_2020/LAZ/USGS_LPC_WY_YellowstoneNP_2020_D20_12TWR400030.laz",
                "type": "application/vnd.laszip",
                "title": "LAS data",
                "roles": [
                    "data"
                ]
            },
            "metadata": {
                "href": "https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/WY_YellowstoneNP_2020_D20/WY_YellowstoneNP_1RF_2020/metadata/USGS_LPC_WY_YellowstoneNP_2020_D20_12TWR400030.xml",
                "type": "application/xml",
                "title": "Metadata",
                "roles": [
                    "metadata"
                ]
            }
        },
        "bbox": [
            -110.49089350974215,
            45.179290023619394,
            2852.31,
            -110.47808425669801,
            45.1883486560674,
            3231.76
        ],
        "stac_extensions": [
            "https://stac-extensions.github.io/pointcloud/v1.0.0/schema.json",
            "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
            "https://stac-extensions.github.io/file/v2.1.0/schema.json"
        ],
        "collection": "WY_YELLOWSTONENP_1RF_2020"
    }
