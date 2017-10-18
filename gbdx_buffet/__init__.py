import argparse
import os
from datetime import datetime
from logging import getLogger

import regex
import sh
from gbdxtools import Interface
from gbdxtools.simpleworkflows import Task, Workflow
from gbdxtools.workflow import Workflow as WorkflowAPI


def download():
    parser = argparse.ArgumentParser()

    parser.add_argument("prefix", help="Prefix of folder to be read")
    parser.add_argument("output", help="Location of export folder")
    parser.add_argument("--dryrun", help="Don't download, just list what will be downloaded", action='store_true')
    parser.add_argument("--verbose", help="verbose", action='store_true')
    args = parser.parse_args()

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
            if args.verbose:
                print(all_folders)
            all_folders = regex.findall(regex.escape(args.prefix) + r'[^ ]*/', str(all_folders))
            print(all_folders)
            print(len(all_folders))
            if not args.dryrun:
                os.makedirs(args.output, exist_ok=True)
                for folder in all_folders:
                    print(folder)
                    print(s3.sync(s3_uri + folder, args.output + folder))
            try_again = False
        except:
            continue


def geofile(file_name):
    import geopandas as gpd
    assert os.path.exists(file_name), "File not found: %s" % file_name
    return gpd.read_file(file_name)


def workflow():
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

    print("Catalog IDs ", catalog_ids)
    orders = gbdx.ordering.location(catalog_ids)
    print(orders)

    for o in orders['acquisitions']:
        w = launch_workflow(o['acquisition_id'], args.name, pansharpen=args.pansharpen, dra=args.dra, wkt=args.wkt)
        print(w.id, w.definition, w.status)


def launch_workflow(cat_id, name, pansharpen=True, dra=True, wkt=None):
    order = gbdx.Task("Auto_Ordering", cat_id=cat_id)
    order.impersonation_allowed = True

    aop = gbdx.Task('AOP_Strip_Processor',
                    data=order.outputs.s3_location.value,
                    enable_pansharpen=pansharpen,
                    enable_acomp=True,
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


gbdx = Interface()
workflow_api = WorkflowAPI()
log = getLogger()