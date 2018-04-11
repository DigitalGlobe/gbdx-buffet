import argparse
import json
import os
from datetime import datetime
from logging import getLogger
from pprint import pprint

import fiona
import regex
import sh
from gbdxtools import Interface
from gbdxtools.simpleworkflows import Task, Workflow
from gbdxtools.workflow import Workflow as WorkflowAPI
from shapely.geometry import shape
from tqdm import tqdm


try:
    gbdx = Interface()
except Exception:
    print("""All operations on GBDX require credentials. You can sign up for a""" +
          """GBDX account at https://gbdx.geobigdata.io. Your GBDX credentials""" +
          """are found under your account profile.""" +
          """gbdxtools expects a config file to exist at ~/.gbdx-config with""" +
          """your credentials. (See formatting for this""" +
          """file here: https://github.com/tdg-platform/gbdx-auth#ini-file.)""")
workflow_api = WorkflowAPI()
log = getLogger()


def download_cli():
    parser = argparse.ArgumentParser()

    parser.add_argument("prefix", help="Prefix of folder to be read")
    parser.add_argument("output", help="Location of export folder")
    parser.add_argument("--dryrun", help="Don't download, just list what will be downloaded", action='store_true')
    parser.add_argument("--verbose", help="verbose", action='store_true')
    args = parser.parse_args()
    download(prefix=args.prefix, output=args.output, verbose=args.verbose, dryrun=args.dryrun)


def download(prefix, output, verbose=False, dryrun=False):
    try_again = True
    gbdx = Interface()
    while try_again:
        try:
            aws = gbdx.s3._load_info()
            os.environ['AWS_ACCESS_KEY_ID'] = aws['S3_access_key']
            os.environ['AWS_SECRET_ACCESS_KEY'] = aws['S3_secret_key']
            os.environ['AWS_SESSION_TOKEN'] = aws['S3_session_token']

            s3 = sh.aws.bake('s3')
            s3_uri = "s3://{}/{}/".format(aws['bucket'], aws['prefix'])

            all_folders = s3("ls", s3_uri).stdout
            if verbose:
                print(all_folders)
            all_folders = regex.findall(regex.escape(prefix) + r'[^ ]*/', str(all_folders))
            print(all_folders)
            print(len(all_folders))
            if not dryrun:
                os.makedirs(output, exist_ok=True)
                for folder in all_folders:
                    print(folder)
                    print(s3.ls(s3_uri + folder, "--recursive", "--human-readable", "--summarize"))
                    print(s3.sync(s3_uri + folder, output + folder, _err_to_out=True, _out_bufsize=100))
            try_again = False
        except Exception as e:
            print(e)
            continue


def geofile(file_name):
    import geopandas as gpd
    assert os.path.exists(file_name), "File not found: %s" % file_name
    return gpd.read_file(file_name)


def workflow_cli():
    parser = argparse.ArgumentParser(description="""Launch a workflow to order images from GBDX""")
    parser.add_argument("-i", "--catids", help="Comma list of CATALOG IDS to be read "
                                               "(10400100175E5C00,104A0100159AFE00,"
                                               "104001002A779400,1040010026627600)", type=lambda s: s.split(','))
    parser.add_argument("-f", "--file", help="File to be read, catid per line (10400100175E5C00\\n104A0100159AFE00\\n"
                                             "104001002A779400\\n1040010026627600)")
    parser.add_argument("-s", "--shapefile", help="Name of shapefile to be read", type=geofile)
    parser.add_argument("-w", "--wkt",
                        help="WKT indicating where to clip images "
                             "e.g. POLYGON ((109.79359281016 18.3095645755021, ....))", default="")
    parser.add_argument("-noa", "--noacomp", help="Disable ACOMP", dest='acomp', action='store_false')
    parser.add_argument("-p", "--pansharpen", help="Enable 4band pansharpening", action='store_true')
    parser.add_argument("-d", "--dra", help="Enable dynamic range adjustment (DRA)", action='store_true')
    parser.add_argument("-n", "--name", help="Name the directory to save images on S3", type=str,
                        default=datetime.now().isoformat().split('T')[0])
    args = parser.parse_args()

    if args.shapefile:
        print("Reading cat ids from shapefile")
        if 'CATALOGID' in args.shapefile.columns:
            catalog_ids = list(args.shapefile.CATALOGID.values)
        elif 'cat_id' in args.shapefile.columns:
            catalog_ids = list(args.shapefile.cat_id.values)
        elif 'catid' in args.shapefile.columns:
            catalog_ids = list(args.shapefile.catid.values)
        else:
            raise Exception("CATALOGID not in shapefile")
    elif args.catids:
        print("Reading cat ids from command line")
        catalog_ids = args.catids
    elif args.file:
        print("Reading cat ids from text file")
        try:
            with open(args.file + 'UND') as f:
                catalog_ids = [s.strip('\n') for s in f.readlines()]
        except FileNotFoundError:
            with open(args.file) as f:
                catalog_ids = [s.strip('\n') for s in f.readlines()]
    else:
        raise Exception("You must provide catalog ids using --shapefile or --catids or --file")

    launch_workflows(catalog_ids, args.name, pansharpen=args.pansharpen, dra=args.dra, acomp=args.acomp, wkt=args.wkt)


def launch_workflows(catalog_ids, name=datetime.now().isoformat().split('T')[0], pansharpen=False, dra=False, acomp=True, wkt=None):
    # print("Catalog IDs ", catalog_ids)
    # print(orders)
    return [launch_workflow(o['acquisition_id'], name, pansharpen=pansharpen, dra=dra, acomp=acomp, wkt=wkt)
            for o in gbdx.ordering.location(catalog_ids)['acquisitions']]


def launch_workflow(cat_id, name, pansharpen=False, dra=False, acomp=True, wkt=None):
    order = gbdx.Task("Auto_Ordering", cat_id=cat_id)
    order.impersonation_allowed = True

    aop = gbdx.Task('AOP_Strip_Processor',
                    data=order.outputs.s3_location.value,
                    enable_pansharpen=pansharpen,
                    enable_acomp=acomp,
                    enable_dra=dra,
                    )

    tasks = [order, aop]

    output = aop.outputs.data
    if wkt:
        tasks.append(Task('RasterClip_Extents', raster=tasks[-1].outputs.data.value, wkt=wkt))
        print(tasks[-1])
        output = tasks[-1].outputs.data
    w = Workflow(tasks)
    w.savedata(output, location=os.path.join(name, cat_id))
    w.execute()
    return w


def check_workflow_cli():
    parser = argparse.ArgumentParser(description="""Launch a workflow to order images from GBDX""")
    parser.add_argument("workflow_ids", help="Comma list of Worflow IDS to be read "
                                             "(4756293649288340653,4756293656541265420,"
                                             "4756293664199537883)", type=lambda s: s.split(','))
    parser.add_argument("-v", "--verbose", help="verbose, otherwise just get state", action='store_true')
    args = parser.parse_args()

    for wid in args.workflow_ids:
        if args.verbose:
            pprint(gbdx.workflow.get(wid))
        else:
            pprint(gbdx.workflow.get(wid)['state'])


class FetchGBDxResults:

    def __init__(self, in_aoi, out_result="", start_date="*", end_date="now", vector_index="vector-*", item_type="all",
                 count=200000):
        self.inAoi = in_aoi
        self.outResult = out_result
        self.vectorIndex = vector_index
        self.itemType = item_type
        self.startDate = start_date
        self.endDate = end_date
        self.maxCount = count

    def merge_geojson(self, in_files):
        out_json = dict(type='FeatureCollection', features=[])

        for in_json in in_files:
            if in_json.get('type', None) != 'FeatureCollection':
                raise Exception('Sorry, "%s" does not look like GeoJSON' % in_json)

            if not isinstance(in_json.get('features', None), list):
                raise Exception('Sorry, "%s" does not look like GeoJSON' % in_json)

            out_json['features'] += in_json['features']

        return out_json

    def extract_detects(self, cli=False):
        if self.itemType == "all":
            self.itemType = "*"

        if isinstance(self.inAoi, dict):
            geoaoi = self.inAoi

        elif os.path.splitext(self.inAoi)[1] == '.geojson':
            with (self.inAoi, 'r') as f:
                geoaoi = json.load(f)
        else:
            geoaoi = fiona.open(self.inAoi)

        if geoaoi is not None:
            all_results = []
            # gbdx = self.getgbdxinterface()
            print('Querying AOIs for {} features.'.format(self.itemType))
            es_query = "item_type: {} & item_date:[{} TO {}]".format(self.itemType, self.startDate, self.endDate)
            pbar = None
            if cli:
                pbar = tqdm(total=len(geoaoi))
            for aoi in geoaoi:
                results = gbdx.vectors.query(str(shape(aoi['geometry'])),
                                             query=es_query,
                                             index=self.vectorIndex,
                                             count=self.maxCount)
                geojson = {
                    'type': 'FeatureCollection',
                    'features': results
                }

                all_results.append(geojson)
                if cli:
                    pbar.update(1)

            merged_geojson = self.merge_geojson(all_results)

            if os.path.isdir(self.outResult):
                now = datetime.now().strftime('%Y-%m-%d')
                out_file = os.path.join(self.outResult, "{}_{}_{}.geojson".format(self.vectorIndex, self.itemType, now))
                with open(out_file, 'w') as output:
                    output.write(json.dumps(merged_geojson))
            else:
                return merged_geojson


def fetch_results_cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--inAoi",
                        help="Input *.shp or *.geojson /path/to/file",
                        type=str,
                        required=True)

    parser.add_argument("-o", "--outDir",
                        help="Geojson /path/to/store/geojson",
                        type=str,
                        required=True)

    parser.add_argument("-vi", "--gbdxVecIndex",
                        default="vector-*",
                        help="Name of GBDx vector index, ex: vector-deepcore-extract-cars*, vector-openskynet*, vector-*",
                        type=str)

    parser.add_argument("-it", "--itemType",
                        default="all",
                        help="Name of object type, options: all, Airliner, Passenger Cars, Aircraft, etc.",
                        type=str)

    parser.add_argument("-std", "--startDate",
                        default="*",
                        help="Beginning date for index query. ex: 2000-01-01 or *",
                        type=str)

    parser.add_argument("-end", "--endDate",
                        default="now",
                        help="End date for index query. ex: 2017-01-01 or now",
                        type=str)

    parser.add_argument("-max", "--maxCount",
                        default=200000,
                        help="Maximum number of features return by GBDx vector index.",
                        type=int)

    args = parser.parse_args()

    assert os.path.isfile(args.inAoi)
    assert os.path.splitext(args.inAoi)[1] == '.shp' or os.path.splitext(args.inAoi)[1] == '.geojson'
    assert os.path.isdir(args.outDir)

    fetcher = FetchGBDxResults(
        args.inAoi,
        args.outDir,
        args.startDate,
        args.endDate,
        args.gbdxVecIndex,
        args.itemType,
        args.maxCount)

    fetcher.extract_detects(cli=True)
