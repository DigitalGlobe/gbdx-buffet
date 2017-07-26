import argparse
from datetime import datetime
from logging import getLogger

import geopandas as gpd
from gbdxtools import Interface
from gbdxtools.simpleworkflows import Workflow, Task
from gbdxtools.workflow import Workflow as WorkflowAPI

log = getLogger()
log.setLevel('DEBUG')

gbdx = Interface()

workflow_api = WorkflowAPI()

parser = argparse.ArgumentParser()
parser.add_argument("--catids", help="Comma list of CATALOG IDS to be read (10400100175E5C00,104A0100159AFE00,"
                                     "104001002A779400,1040010026627600)", type=lambda s: s.split(','))
parser.add_argument("--catfile", help="File to be read, catid per line (10400100175E5C00,104A0100159AFE00,"
                                      "104001002A779400,1040010026627600)")
parser.add_argument("--shapefile", help="Name of shapefile to be read", type=lambda s: gpd.read_file(s))

parser.add_argument("--wkt", help='POLYGON ((109.79359281016 18.3095645755021, ....))')
parser.add_argument("--pansharpen", help="Enable 4band pansharpening", type=bool, default=False)
args = parser.parse_args()

UNDELIVERED = 'UND' if args.catfile is None else args.catfile + "UND"
DELIVERED = 'DELIV' if args.catfile is None else args.catfile + "DELIV"

def launch_workflow(location):
    tasks = [
        (Task("AOP_Strip_Processor", data=location, enable_acomp=True,
              enable_pansharpen=args.pansharpen, enable_dra=False, ))
    ]
    if args.wkt:
        tasks.append(Task('CropGeotiff', data=tasks[0].outputs.data.value, wkt=args.wkt))
    w = Workflow(tasks)
    today, _ = datetime.now().isoformat().split('T')
    w.savedata(tasks[-1].outputs.data, location=datetime.now().isoformat())
    w.execute()
    return w


def main():
    workflows = []
    if args.shapefile:
        if 'CATALOGID' not in args.shapefile.columns:
            raise Exception("CATALOGID not in shapefile")
        else:
            catalog_ids = list(args.shapefile.CATALOGID)
    elif args.catids:
        catalog_ids = args.catids
    elif args.catfile:
        try:
            with open(args.catfile + 'UND') as f:
                catalog_ids = [s.strip('\n') for s in f.readlines()]
        except FileNotFoundError:
            with open(args.catfile) as f:
                catalog_ids = [s.strip('\n') for s in f.readlines()]
    else:
        raise Exception("Choose --shapefile or --catids or --catfile")

    orders = gbdx.ordering.location(catalog_ids)
    delivered = [o for o in orders['acquisitions'] if o['state'] == 'delivered']

    with open(DELIVERED, 'w') as f:
        for o in delivered:
            w = launch_workflow(o['location'])
            print(w.id, w.definition, w.status)
            workflows.append(w)
            f.write(o['acquisition_id'])

    undelivered = [o['acquisition_id'] + '\n' for o in orders['acquisitions'] if o['state'] != 'delivered']
    with open(UNDELIVERED, 'w') as f:
        f.writelines(undelivered)

if __name__ == '__main__':
    main()
