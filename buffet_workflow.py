import argparse
import os
from datetime import datetime
from logging import getLogger

from gbdxtools import Interface
from gbdxtools.simpleworkflows import Workflow, Task
from gbdxtools.workflow import Workflow as WorkflowAPI
import geopandas as gpd

log = getLogger()
log.setLevel('DEBUG')

gbdx = Interface()

workflow_api = WorkflowAPI()


def geofile(file_name):
    assert os.path.exists(file_name), "File not found: %s"%file_name
    return gpd.read_file(file_name)


def main():
    parser = argparse.ArgumentParser(description="""Launch a workflow to order images from GBDX""")
    parser.add_argument("-i", "--catids", help="Comma list of CATALOG IDS to be read (10400100175E5C00,104A0100159AFE00,"
                                         "104001002A779400,1040010026627600)", type=lambda s: s.split(','))
    parser.add_argument("-f", "--file", help="File to be read, catid per line (10400100175E5C00\\n104A0100159AFE00\\n"
                                          "104001002A779400\\n1040010026627600)")
    parser.add_argument("-s", "--shapefile", help="Name of shapefile to be read", type=geofile)
    parser.add_argument("-w", "--wkt", help="WKT indicating where to clip images e.g. POLYGON ((109.79359281016 18.3095645755021, ....))", default="")
    parser.add_argument("-p", "--pansharpen", help="Enable 4band pansharpening", action='store_true')
    parser.add_argument("-d", "--dra", help="Enable dynamic range adjustment (DRA)", action='store_true')
    parser.add_argument("-n", "--name", help="Name the directory to save images on S3", type=str, default=datetime.now().isoformat().split('T')[0])
    args = parser.parse_args()

    DELIVERED = 'DELIV' if args.file is None else args.file + "DELIV"

    workflows = []
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

    orders = gbdx.ordering.location(catalog_ids)
    print(orders)

    with open(DELIVERED, 'w') as f:
        for o in orders['acquisitions']:
            w = launch_workflow(o['acquisition_id'], args.name, pansharpen=args.pansharpen, dra=args.dra, wkt=args.wkt)
            print(w.id, w.definition, w.status)
            workflows.append(w)
            f.write(o['acquisition_id'] + '/n')


def launch_workflow(cat_id, name, pansharpen=True, dra=True, wkt=None):
    order = gbdx.Task("Auto_Ordering", cat_id=cat_id)
    order.impersonation_allowed = True

    aop = gbdx.Task('AOP_Strip_Processor',
                    data=order.outputs.s3_location.value,
                    enable_pansharpen=pansharpen,
                    enable_acomp=True,
                    enable_dra=dra,
                    # ortho_epsg='EPSG:4326'
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


if __name__ == '__main__':
    main()
