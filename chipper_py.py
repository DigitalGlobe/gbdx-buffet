import argparse
import errno
import logging
import multiprocessing as mp
import os
import subprocess

import drago
import gbdxtools
import geopandas as gpd
import ogr
import requests
from PIL import Image
from gbdxtools.idaho import Idaho
from os.path import join

ERROR_FILE = "imagery_not_available_log.txt"

verbose = False

logging_level = logging.INFO if not verbose else logging.DEBUG
message_format = "%(asctime)s %(levelname)s: %(funcName)s: %(message)s"
date_format = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(level=logging_level, format=message_format, datefmt=date_format)

# 3rd Party loggers
logging.getLogger("gbdxtools").setLevel(logging.DEBUG)
# logging.getLogger("requests").setLevel(logging.WARNING)

logger = logging.getLogger()
logger.addHandler(logging.NullHandler())

min_free_space = int(25e9)  # 25GB

DATA_DIR = "data"
LABEL_DIR = "labels"

CLASS_DIR = "Powerstations"
NO_CLASS_DIR = "None"


class IdahoM(Idaho):
    gbdx_basename = "https://idaho.geobigdata.io/"

    def get_chip_from_wkt(self, wkt, catid, height=768, width=768, chip_type='PS',chip_format='TIF', filename='chip.tif'):
        """Downloads a native resolution, orthorectified chip in tif format
        from a user-specified catalog id.
        Args:
            lat (float): Decimal degree latitude of centroid.
            lon (float): Decimal degree longitude of centroid.
            catid (str): The image catalog id.
            chip_type (str): 'PAN' (panchromatic), 'MS' (multispectral), 'PS'
                  (pansharpened).  'MS' is 4 or 8 bands depending on sensor.
            chip_format (str): 'TIF' or 'PNG'
            filename (str): Where to save chip.
        Returns:
            True if chip is successfully downloaded; else False.
            :param chip_type:
            :param width:
            :param height:
        """
        results = self.get_images_by_catid_and_aoi(catid=catid, aoi_wkt=wkt)
        description = self.describe_images(results)

        pan_id, ms_id, num_bands = None, None, 0
        for catid, images in description.items():
            for partnum, part in images['parts'].items():
                if 'PAN' in part.keys():
                    pan_id = part['PAN']['id']
                if 'WORLDVIEW_8_BAND' in part.keys():
                    ms_id = part['WORLDVIEW_8_BAND']['id']
                    num_bands = 8
                elif 'RGBN' in part.keys():
                    ms_id = part['RGBN']['id']
                    num_bands = 4

        # specify band information
        bands = ''
        if chip_type == 'PAN':
            if not pan_id:
                # If image is missing an identifier for this type
                return False
            bands = pan_id + '?bands=0'
        elif chip_type == 'MS':
            if not pan_id:
                # If image is missing an identifier for this type
                return False
            bands = ms_id + '?'
        elif chip_type in ['PS', 'PS8']:
            if not ms_id:
                return False
            elif not pan_id:
                return False
            if num_bands == 8:
                if chip_type == 'PS8':
                    bands = ms_id + '?bands=4,2,1,0,3,5,6,7&panId=' + pan_id
                else:
                    bands = ms_id + '?bands=4,2,1&panId=' + pan_id
            elif num_bands == 4:
                band = ms_id + '?bands=0,1,2&panId=' + pan_id

        # Get BBOX
        geometry = ogr.CreateGeometryFromWkt(wkt)
        envelope = geometry.GetEnvelope()
        lower_right = "{},{}".format(envelope[1], envelope[2])
        upper_left = "{},{}".format(envelope[0], envelope[3])

        # specify location information
        endpoint = self.gbdx_basename + 'v1/chip/bbox/idaho-images/'
        location = '&upperLeft={}&lowerRight={}'.format(upper_left, lower_right)
        image = '&width={}&height={}&format={}'.format(width, height, chip_format)
        # processing  = '&doDRA'.format()
        auth = '&token={}'.format(self.gbdx_connection.access_token)

        url = "".join([endpoint, bands, location, image, auth])
        r = requests.get(url)
        logging.debug("Image download url: {}".format(url))

        if r.status_code == 200:
            if filename:
                with open(filename, 'wb') as f:
                    f.write(r.content)
                    return True
            else:
                return r.content
        else:
            logging.debug('Cannot download chip. Status code {}'.format(r.status_code))
            return False

    def get_chip_from_centroid(
            self, lat, lon, catid, height=256, width=256, chip_type='PS',
            chip_format='TIF', filename='chip.tif'):
        """Downloads a native resolution, orthorectified chip in tif format
        from a user-specified catalog id.
        Args:
            lat (float): Decimal degree latitude of centroid.
            lon (float): Decimal degree longitude of centroid.
            catid (str): The image catalog id.
            chip_type (str): 'PAN' (panchromatic), 'MS' (multispectral), 'PS'
                  (pansharpened), 'PS8' (pansharpened 8-band).
                  'MS' is 4 or 8 bands depending on sensor.
            chip_format (str): 'TIF' or 'PNG'
            filename (str): Where to save chip.
        Returns:
            True if chip is successfully downloaded; else False.
        """
        point = "{lon} {lat}".format(lon=lon, lat=lat)
        box_wkt = "POLYGON (({point}, {point}, {point}, {point}, {point}))".format(point=point)

        results = self.get_images_by_catid_and_aoi(catid=catid, aoi_wkt=box_wkt)
        description = self.describe_images(results)

        pan_id, ms_id, num_bands = None, None, 0
        for catid, images in description.items():
            for partnum, part in images['parts'].items():
                if 'PAN' in part.keys():
                    pan_id = part['PAN']['id']
                if 'WORLDVIEW_8_BAND' in part.keys():
                    ms_id = part['WORLDVIEW_8_BAND']['id']
                    num_bands = 8
                elif 'RGBN' in part.keys():
                    ms_id = part['RGBN']['id']
                    num_bands = 4

        # specify band information
        bands = ''
        if chip_type == 'PAN':
            if not pan_id:
                # If image is missing an identifier for this type
                return False
            bands = pan_id + '?bands=0'
        elif chip_type == 'MS':
            if not pan_id:
                # If image is missing an identifier for this type
                return False
            bands = ms_id + '?'
        elif chip_type in ['PS', 'PS8']:
            if not ms_id:
                return False
            elif not pan_id:
                return False
            if num_bands == 8:
                if chip_type == 'PS8':
                    bands = ms_id + '?bands=4,2,1,0,3,5,6,7&panId=' + pan_id
                else:
                    bands = ms_id + '?bands=4,2,1&panId=' + pan_id
                    logging.debug("bands=4,2,1")
            elif num_bands == 4:
                band = ms_id + '?bands=0,1,2&panId=' + pan_id
                logging.debug("bands=0,1,2")

        # specify location information
        endpoint = self.gbdx_basename + 'v1/chip/centroid/idaho-images/'
        location = '&lat={}&long={}'.format(lat, lon)
        image = '&width={}&height={}&format={}'.format(width, height, chip_format)
        processing = '&doDRA'.format()
        auth = '&token={}'.format(self.gbdx_connection.access_token)

        url = "".join([endpoint, bands, location, image, auth])
        r = requests.get(url)
        logging.debug("Image download url: {}".format(url))

        if r.status_code == 200:
            if filename:
                with open(filename, 'wb') as f:
                    f.write(r.content)
                    return True
            else:
                return r.content
        else:
            logging.debug('Cannot download chip. Status code {}'.format(r.status_code))
            return False

# def _amend_gbdx(gbdx)
#     gbdx.idaho.get_chip_from_centroid = types.MethodType(get_chip_from_centroid, gbdx.idaho)
#     gbdx.idaho.get_chip_from_wkt = types.MethodType(get_chip_from_wkt, gbdx.idaho)

def free_space(path):
    stats = os.statvfs(path)
    space = stats.f_bavail * stats.f_frsize
    return space


def check_and_make_dir(path):
    """ Check for the existance of a directory and create if it does not """
    if not os.path.isdir(path):
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
    return True


def label_worker(work_queue, label_shapefile, output_path="."):
    pid = mp.current_process()
    logging.info("Started process {}".format(pid))

    features_df = gpd.read_file(label_shapefile)

    while free_space(output_path) > min_free_space:
        image_path, = work_queue.get(True)

        # get label
        tiff = drago.imagery.TiffImage(image_path)
        imcoords = tiff.geopandas2imcoords(features_df)
        mim = drago.markedimage.MarkedImage(tiff.load_image(), imcoords)
        label = Image.fromarray(mim.mask())  # .astype('uint8')

        # If label is black then there was no intersection -> None; Else -> Powerstation
        label_gray = label.convert("L")
        if label_gray.getextrema() == (0, 0):
            class_name = NO_CLASS_DIR
        else:
            class_name = CLASS_DIR

        # save mask as png in
        png_name, = os.path.basename(image_path).split(".")
        png_name += ".png"

        label_class_path = os.path.join(output_path, LABEL_DIR, class_name)
        check_and_make_dir(label_class_path)
        label_png_path = join(label_class_path, png_name)
        label.save(label_png_path, "PNG")

        # convert tif to png
        image_class_path = os.path.join(output_path, DATA_DIR, class_name)
        check_and_make_dir(image_class_path)
        image_png_path = os.path.join(image_class_path, png_name)
        result = subprocess.check_output(
            ['gdal_translate', '-ot', 'Byte', '-scale', '-b', '1', '-b', '2', '-b', '3', '-of', 'PNG',
             image_path, image_png_path]
        )
        print(result)

        work_queue.task_done()


def download_worker(gbdx, work_queue, not_available_queue, label_queue, output_path="."):
    pid = mp.current_process()
    logging.info("Started process {}".format(pid))

    while free_space(output_path) > min_free_space:
        payload = work_queue.get(True)
        catalog_id = payload[0]
        latitude = payload[1]
        longitude = payload[2]
        chip_path = payload[3]

        print(chip_path)

        size = 2048
        content = gbdx.idaho.get_chip_from_centroid(latitude, longitude,
                                                    catalog_id, height=size, width=size, filename=chip_path,
                                                    chip_type='PS8')

        # TODO?: Possible UTM reprojection
        # gdal.Warp(chip_name, chip_name, dstSRS='EPSG:4326')

        if not content:
            not_available_queue.put(payload)
        else:
            logging.info("Got chip {}".format(chip_path))
            # Send to label maker
            payload = [chip_path]
            label_queue.put(payload)

        work_queue.task_done()


def withhold_data(path, withheld_amount=0.10):
    import random
    import shutil

    def _get_pngs(path):
        files = []
        for f in os.listdir(path):
            if f.endswith('.png'):
                files.extend(f)
        return files

    # Get list of images in Powerstation class
    tagged_image_path = os.path.join(path, DATA_DIR, CLASS_DIR)
    tagged_image_names = _get_pngs(tagged_image_path)

    # Get list of images in None class
    none_image_path = os.path.join(path, DATA_DIR, NO_CLASS_DIR)
    none_image_names = _get_pngs(none_image_path)

    # Shuffle up the images to avoid bias
    random.shuffle(tagged_image_names)
    random.shuffle(none_image_names)

    # Select images for train/test sets
    test_amount = int(len(tagged_image_names) * withheld_amount)

    test_tagged_image_names = tagged_image_names[0:test_amount]
    test_none_image_names = none_image_names[0:test_amount]
    train_image_names = tagged_image_names[test_amount:]

    # Copy images and labels to new destination
    TEST_DIR = "test"
    TRAIN_DIR = "train"

    test_class_path = os.path.join(path, TEST_DIR, CLASS_DIR)
    test_no_class_path = os.path.join(path, TEST_DIR, NO_CLASS_DIR)

    for image in test_tagged_image_names:
        source_path = os.path.join(tagged_image_path, image)
        destination_path = os.path.join(test_class_path, image)
        shutil.copyfile(source_path, destination_path)

    for image in test_none_image_names:
        source_path = os.path.join(none_image_path, image)
        destination_path = os.path.join(test_no_class_path, image)
        shutil.copyfile(source_path, destination_path)

    # train

    for image in train_image_names:
        source_path = os.path.join(tagged_image_path, image)
        destination_path = os.path.join(test_no_class_path, image)
        shutil.copyfile(source_path, destination_path)

def error(error_txt):
    with open(ERROR_FILE, 'wa') as f:
        f.write("{},".format(error_txt))

def main(threads, poi_shapefile, label_shapefile, output_path):
    gbdx = gbdxtools.Interface()
    gbdx.idaho = IdahoM()

    # make the output directory if it doesn't exist
    check_and_make_dir(output_path)

    # Load POI shapefile
    poi_df = gpd.read_file(poi_shapefile)
    print(poi_df.columns)
    # Set up multiprocessing
    label_queue = mp.JoinableQueue()
    label_pool = mp.Pool(1, label_worker, (label_queue, label_shapefile, output_path))

    not_available_queue = mp.Queue()
    download_queue = mp.JoinableQueue()
    download_pool = mp.Pool(threads, download_worker, (gbdx, download_queue,
                                                       not_available_queue, label_queue, output_path))
    while free_space(output_path) > min_free_space:
        payload = download_queue.get(True)
        catalog_id = payload[0]
        latitude = payload[1]
        longitude = payload[2]
        chip_path = payload[3]

        print(chip_path)

        size = 2048
        content = gbdx.idaho.get_chip_from_centroid(latitude, longitude,
                                                    catalog_id, height=size, width=size, filename=chip_path,
                                                    chip_type='PS8')

        # TODO?: Possible UTM reprojection
        # gdal.Warp(chip_name, chip_name, dstSRS='EPSG:4326')

        if not content:
            error(payload)
        else:
            logging.info("Got chip {}".format(chip_path))
            # Send to label maker
            label_queue.put([chip_path])

        download_queue.task_done()

    features_df = gpd.read_file(label_shapefile)

    while free_space(output_path) > min_free_space:
        image_path, = label_queue.get(True)

        # get label
        tiff = drago.imagery.TiffImage(image_path)
        imcoords = tiff.geopandas2imcoords(features_df)
        mim = drago.markedimage.MarkedImage(tiff.load_image(), imcoords)
        label = Image.fromarray(mim.mask())  # .astype('uint8')

        # If label is black then there was no intersection -> None; Else -> Powerstation
        label_gray = label.convert("L")
        if label_gray.getextrema() == (0, 0):
            class_name = NO_CLASS_DIR
        else:
            class_name = CLASS_DIR

        # save mask as png in
        image_png_name, = os.path.basename(image_path).split(".")
        image_png_name += ".png"

        label_class_path = os.path.join(output_path, LABEL_DIR, class_name)
        check_and_make_dir(label_class_path)
        label_png_path = os.path.join(label_class_path, image_png_name)
        label.save(label_png_path, "PNG")

        # convert tif to png
        image_class_path = os.path.join(output_path, DATA_DIR, class_name)
        check_and_make_dir(image_class_path)
        image_png_path = os.path.join(image_class_path, image_png_name)
        result = subprocess.check_output(
            ['gdal_translate', '-ot', 'Byte', '-scale', '-b', '1', '-b', '2', '-b', '3', '-of', 'PNG',
             image_path, image_png_path]
        )
        print(result)

        label_queue.task_done()

    # Iterate through features of POI shapefile, place on download queue
    for num, value in enumerate(poi_df.values):
        latitude = value[1]
        longitude = value[0]
        catalog_id = value[3]
        chip_path = os.path.join(output_path, 'tifs', "{}.TIF".format(num))

        check_and_make_dir(os.path.join(output_path, 'tifs'))

        # TODO: implement ordering?

        payload = [catalog_id, latitude, longitude, chip_path]
        download_queue.put(payload)

    download_queue.join()
    download_queue.close()
    label_queue.join()
    label_queue.close()

    with open("imagery_not_available_log.txt", 'w') as f:
        while not_available_queue.qsize() > 0:
            payload = not_available_queue.get(True)
            error_string = "{},".format(payload)
            f.write(error_string)

    download_queue.close()

    download_pool.close()
    download_pool.join()
    label_pool.close()
    label_pool.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--poi_shapefile', '-p', help='path to the shapefile', type=str, required=True)
    parser.add_argument('--label_shapefile', '-l', help='path to the shapefile', type=str)
    parser.add_argument('--output_path', '-o', default=".", help='path to output directory', type=str)
    parser.add_argument('--threads', '-t', default=1, help='number of threads used to access gbdx imagery', type=int)
    args = parser.parse_args()
    if not args.label_shapefile:
        args.label_shapefile = args.poi_shapefile

    main(args.threads, args.poi_shapefile, args.label_shapefile, args.output_path)
