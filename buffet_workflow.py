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

UNDELIVERED = 'UND' if args.file is None else args.file + "UND"
DELIVERED = 'DELIV' if args.file is None else args.file + "DELIV"


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
        tasks.append(Task('RasterClip_Extents', raster=tasks[-1].outputs.data.value, wkt=args.wkt))
        print(tasks[-1])
        output = tasks[-1].outputs.data
    w = Workflow(tasks)
    today, _ = datetime.now().isoformat().split('T')
    w.savedata(output, location=os.path.join(today, cat_id))
    w.execute()
    return w


def main():
    workflows = []
    if args.shape:
        if 'CATALOGID' not in args.shape.columns:
            raise Exception("CATALOGID not in shapefile")
        else:
            catalog_ids = list(args.shapefile.CATALOGID)
    elif args.catids:
        catalog_ids = args.catids
    elif args.file:
        try:
            with open(args.file + 'UND') as f:
                catalog_ids = [s.strip('\n') for s in f.readlines()]
        except FileNotFoundError:
            with open(args.file) as f:
                catalog_ids = [s.strip('\n') for s in f.readlines()]
    else:
        raise Exception("Choose --shape or --catids or --file")

    orders = gbdx.ordering.location(catalog_ids)
    print(orders)
    delivered = [o for o in orders['acquisitions'] if o['state'] == 'delivered']

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
