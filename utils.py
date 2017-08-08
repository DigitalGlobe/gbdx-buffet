import argparse


def get_parser():

    def open_shape(f):
        import geopandas
        return geopandas.read_file(f)

    parser = argparse.ArgumentParser()
    parser.add_argument("--catids", help="Comma list of CATALOG IDS to be read (10400100175E5C00,104A0100159AFE00,"
                                         "104001002A779400,1040010026627600)", type=lambda s: s.split(','))
    parser.add_argument("--catfile", help="File to be read, catid per line (10400100175E5C00,104A0100159AFE00,"
                                          "104001002A779400,1040010026627600)")
    parser.add_argument("--shapefile", help="Name of shapefile to be read", type=open_shape)
    parser.add_argument("--wkt", help='POLYGON ((109.79359281016 18.3095645755021, ....))')
    parser.add_argument("--pansharpen", help="Enable 4band pansharpening", type=bool, default=False)
    return parser