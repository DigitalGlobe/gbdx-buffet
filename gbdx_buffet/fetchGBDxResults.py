"""
Author: Andrew Jenkins
Date: 12NOV17
Company: Radiant Solutions
Description: Extract object detections from GBDx vector indexes
             that spatially intersect user-defined AOIs.
"""

import os
import json
from datetime import datetime
try:
    import argparse
    from shapely.geometry import shape
    from tqdm import tqdm
    import fiona
    import gbdxtools
except ImportError as err:
    not_found = str(err).split(' ')[-1].replace("'", "")
    print('{} not installed. run: pip3 install {}'.format(not_found, not_found))


class FetchGBDxResults:

    def __init__(self, in_aoi, out_result="", start_date="*", end_date="now", vector_index="vector-*", item_type="all", count=200000):
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

    def getgbdxinterface(self):
        try:
            gbdx = gbdxtools.Interface()
            return gbdx
        except Exception:
            print("""All operations on GBDX require credentials. You can sign up for a""" +
                  """GBDX account at https://gbdx.geobigdata.io. Your GBDX credentials""" +
                  """are found under your account profile.""" +
                  """gbdxtools expects a config file to exist at ~/.gbdx-config with""" +
                  """your credentials. (See formatting for this""" +
                  """file here: https://github.com/tdg-platform/gbdx-auth#ini-file.)""")

    def extract_detects(self):
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
            gbdx = self.getgbdxinterface()
            print('Querying AOIs for {} features.'.format(self.itemType))
            es_query = "item_type: {} & item_date:[{} TO {}]".format(self.itemType, self.startDate, self.endDate)
            if parser is not None: pbar = tqdm(total=len(geoaoi))
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
                if tqdm is not None: pbar.update(1)

            merged_geojson = self.merge_geojson(all_results)

            if os.path.isdir(self.outResult):
                now = datetime.now().strftime('%Y-%m-%d')
                out_file = os.path.join(self.outResult, "{}_{}_{}.geojson".format(self.vectorIndex, self.itemType, now))
                with open(out_file, 'w') as output:
                    output.write(json.dumps(merged_geojson))
            else:
                return merged_geojson

if __name__ == '__main__':
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
                        help="Name of object type, options: all, Airliner, Passenger Cars, Fighter Aircraft, etc.",
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

    fetcher.extract_detects()