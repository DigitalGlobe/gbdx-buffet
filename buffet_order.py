import argparse
from logging import getLogger

import geopandas as gpd
from gbdxtools import Interface
from gbdxtools.workflow import Workflow as WorkflowAPI

log = getLogger()
log.setLevel('INFO')

gbdx = Interface()

workflow_api = WorkflowAPI()

parser = argparse.ArgumentParser()
parser.add_argument("--catids", help="Comma list of CATALOG IDS to be read (10400100175E5C00,104A0100159AFE00,"
                                     "104001002A779400,1040010026627600)", type=lambda s: s.split(','))
parser.add_argument("--catfile", help="File to be read, catid per line (10400100175E5C00,104A0100159AFE00,"
                                     "104001002A779400,1040010026627600)")
parser.add_argument("--shapefile", help="Name of shapefile to be read")
parser.add_argument("--output", help="Location of export datafile", default='')
parser.add_argument("--geometryfield", help="the geometry feature name", default='geometry')
parser.add_argument("--cloudcover", type=float, help="percentage of cloud cover (e.g. --cloudcover 10)")
parser.add_argument("--available", type=bool, help="use imagery available in gbdx")
parser.add_argument("--sensors",
                    help="--sensors WORLDVIEW02 --sensors WORLDVIEW03)", action='append')
parser.add_argument("--offnadir", type=float, help=" Maximum off-nadir angle")
parser.add_argument("--startdate", help="Start date (eg 2017/03/31")
parser.add_argument("--enddate", help="End date (eg 2017/03/31")
parser.add_argument("--sql", help=" (eg SELECT * FROM ....")
parser.add_argument("--pansharpen", help="Enable 4band pansharpening", type=bool, default=False)
args = parser.parse_args()


def main():
    if args.shapefile:
        master = gpd.read_file(args.shapefile)

        if 'CATALOGID' not in master.columns:
            raise Exception("CATALOGID is not in the shapefile, use buffet_search.py first")
        else:
            master.index = master.CATALOGID

    elif args.catids:
        master = gpd.GeoDataFrame({"CATALOGID": args.catids})
        master.index = master.CATALOGID
    elif args.catfile:
        with open(args.catfile) as f:
            catids = [s.strip('\n') for s in f.readlines()]
            print(catids)
        master = gpd.GeoDataFrame({"CATALOGID": catids})
        master.index = master.CATALOGID
    else:
        raise FileNotFoundError()

    order_id = gbdx.ordering.order(list(master.index))
    log.info("Ordered with order number: {}".format(order_id))

    orders = gbdx.ordering.location(list(master.index))
    delivered = [o for o in orders['acquisitions'] if o['state'] == 'delivered']
    undelivered = [o['acquisition_id'] + '\n' for o in orders['acquisitions'] if o['state'] != 'delivered']

    print("DELIVERED")
    for delivery in delivered:
        print(delivery)

    print('UNDELIVERED')
    for undelivery in undelivered:
        print(undelivery)

if __name__ == '__main__':
    main()
