import argparse
from datetime import datetime
from logging import getLogger
from os.path import join
from threading import Thread
from time import sleep
from typing import List

import geopandas as gpd
import requests
import shapely.wkt
from gbdxtools import Interface
from gbdxtools.simpleworkflows import Workflow, Task
from gbdxtools.workflow import Workflow as WorkflowAPI
from geopandas import GeoDataFrame

log = getLogger()
log.setLevel('DEBUG')

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

def keep_trying(func, **kwargs):
    keep_searching = True
    while keep_searching:
        try:
            return func(**kwargs)
        except requests.exceptions.HTTPError:
            keep_searching = True

def gen_query(args):
    filters = []
    if args.cloudcover:
        print("cloudCover: {}".format(args.cloudcover))
        filters.append("cloudCover < {}".format(args.cloudcover))
    if args.sensors:
        filters.append("(" + " OR ".join(args.sensors.split(';')) + ")")
    if args.offnadir:
        filters.append("offNadirAngle {}".format(args.offnadir))

    def date2utc(date):  # Example: '2017-01-01T24:52:38.000Z'
        if date:
            return "{}-{}-{}T00:00:00.000Z".format(*date.split('/'))

    if args.available:
        filters.append("available = true")
    query_params = {"startDate": date2utc(args.startdate), "endDate": date2utc(args.enddate), "filters": filters}
    return query_params


def catalog_search(wkt):
    all_results = []
    results = keep_trying(gbdx.catalog.search, searchAreaWkt=wkt,
                          types=['DigitalGlobeAcquisition'])  # , **query_params))
    log.debug('{} images this AOI'.format(len(results)))
    print("\nNumber of images: {}".format(len(results)))
    for result in results[:1]:
        image = result['properties']
        image['catid'] = result['identifier']
        image['type'] = '|'.join(result['type'])
        image['query_aoi'] = wkt
        image['geometry'] = shapely.wkt.loads(image['footprintWkt'])

        all_results.append(image)
    return all_results


def workflow_status(df:GeoDataFrame, workflows: List[Workflow]):

    workflow_ids = gbdx.gbdx_connection.get('https://geobigdata.io/workflows/v1/workflows').json().get('Workflows')
    print(workflow_ids)
    for workflow_id in workflow_ids:
        print("prev", workflow_id, workflow_api.status(workflow_id))

    while True:
        print()
        for w in workflows:
            if isinstance(w, Workflow):
                print(w.id, w.status)
            elif isinstance(w, bytes):
                print("prev", w, workflow_api.status(str(w)), workflow_api.get(str(w)))
            sleep(5)

def order_status(df: GeoDataFrame, order_id: str, workflows: List[Workflow]):
    """
    loop through the status and check status. store a status number in the master df, after loop,
    [{'acquisition_id': '104001001ED23100', 'state': 'submitted', 'location': 'not_delivered'}]
    [{'acquisition_id': '104001001ED23100',
                   'state': 'delivered', 'location': 's3://receiving-dgcs-tdgplatform-com/056721940010_01_003'}]
    :param workflows:
    :param df:
    :param order_id:
    :return:
    """

    # todo save the master, launch the Workflow process
    def order_changed(prev, curr):
        return len(prev) != len(curr) or any(x != y for x, y in zip(prev, curr))

    not_delivered = True
    status = {}

    df['state'] = ""
    df['location'] = ""
    while not_delivered:
        previous_status = status
        status = gbdx.ordering.status(order_id)

        # check if any orders are still pending to continue the loop
        not_delivered = any(order['state'] != 'delivered' for order in status)

        status = {o["acquisition_id"]: o for o in status}

        # check if status changed from last check, if so update master dataframe
        if order_changed(previous_status, status):
            update = gpd.pd.DataFrame(status).T  # .set_index('acquisition_id')
            # update.index.name = 'catid'

            # update master dataframe
            df.update(update)

            for cat_id in status:
                if status[cat_id] != previous_status.get(cat_id):
                    o = status[cat_id]

                    print(o)
                    w = launch_workflow(o['location'])
                    # requests.exceptions.HTTPError: 504 Server Error: GATEWAY_TIMEOUT for url: https://geobigdata.io/workflows/v1/workflows
                    print(w.id, w.definition, w.status)
                    workflows.append(w)


            # todo create Workflow for newly delivered image
        sleep(3)


def launch_workflow(location):
    t = Task("AOP_Strip_Processor", data=location,
             enable_acomp=True, enable_pansharpen=args.pansharpen, enable_dra=False,
             )
    w = Workflow([t])
    # todo make location a parameter
    today, _ = datetime.now().isoformat().split('T')
    w.savedata(t.outputs.data, location=datetime.now().isoformat())
    w.execute()
    return w


def main():

    dbscan_sql = "SELECT feature_id, ST_ClusterDBSCAN(axis_bbox, eps := 300, minPoints := 5) OVER(ORDER BY feature_id) AS cluster_id FROM spacenet_mod WHERE type_id=18;"

    workflows = []
    if args.shapefile:
        df = gpd.read_file(args.shapefile)

        if 'CATALOGID' not in df.columns:
            all_results = []
            filename = 'master'
            for ind, row in df.iterrows():
                wkt = row[args.geometryfield].to_wkt()

                all_results.extend(catalog_search(wkt))

            master = gpd.GeoDataFrame(all_results, crs={'init': 'epsg:4326'}, geometry='geometry')
            master.set_index('catid', inplace=True)
            master.to_file(join(args.output, filename))

            with open(join(args.output, filename + '.json'), 'w') as f:
                f.write(master.to_json())

            # Todo this needs to be chunked into multiple orders

        else:
            master = df
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

    if args.orderonly:
        order_id = gbdx.ordering.order([cat_id for cat_id in master.index])
        log.info("Ordered with order number: {}".format(order_id))

    if args.workflow:
        orders = gbdx.ordering.location(master.index)
        for o in orders['acquisitions']:
            if o['state'] == 'delivered':
                w = launch_workflow(o['location'])
                # requests.exceptions.HTTPError: 504 Server Error: GATEWAY_TIMEOUT for url: https://geobigdata.io/workflows/v1/workflows
                print(w.id, w.definition, w.status)
                print(o)
                workflows.append(w)

    # workflow_thread = Thread(target=workflow_status, args=(master, workflows))
    # workflow_thread.start()
    #
    # order_id = gbdx.ordering.order([cat_id for cat_id in master.index])
    # log.info("Ordered with order number: {}".format(order_id))
    #
    # status_thread = Thread(target=order_status, args=(master, order_id, workflows))
    # status_thread.start()


if __name__ == '__main__':
    main()
