import os
from datetime import datetime
from logging import getLogger

from gbdxtools import Interface
from gbdxtools.simpleworkflows import Workflow, Task
from gbdxtools.workflow import Workflow as WorkflowAPI

from utils import get_parser

log = getLogger()
log.setLevel('DEBUG')

gbdx = Interface()

workflow_api = WorkflowAPI()

parser = get_parser()
args = parser.parse_args()

UNDELIVERED = 'UND' if args.catfile is None else args.catfile + "UND"
DELIVERED = 'DELIV' if args.catfile is None else args.catfile + "DELIV"


def launch_workflow(location, cat_id):
    order = gbdx.Task("Auto_Ordering", cat_id=cat_id)
    order.impersonation_allowed = True

    aop = gbdx.Task('AOP_Strip_Processor',
                    data=order.outputs.s3_location.value,
                    enable_pansharpen=args.pansharpen,
                    enable_acomp=True,
                    enable_dra=False,
                    # ortho_epsg='EPSG:4326'
                    )

    tasks = [order, aop]

    output = aop.outputs.data
    if args.wkt:
        tasks.append(Task('rasterclip_extents', data=tasks[-1].outputs.data.value, wkt=args.wkt))
        output = tasks[-1].s3_location.data
    w = Workflow(tasks)
    today, _ = datetime.now().isoformat().split('T')
    w.savedata(output, location=os.path.join(today, cat_id))
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
    delivered = [o + "\n" for o in orders['acquisitions'] if o['state'] == 'delivered']

    with open(DELIVERED, 'w') as f:
        for o in delivered:
            w = launch_workflow(o['location'], o['acquisition_id'])
            print(w.id, w.definition, w.status)
            workflows.append(w)
            f.write(o['acquisition_id'])

    undelivered = [o['acquisition_id'] + '\n' for o in orders['acquisitions'] if o['state'] != 'delivered']
    with open(UNDELIVERED, 'w') as f:
        f.writelines(undelivered)


if __name__ == '__main__':
    main()
