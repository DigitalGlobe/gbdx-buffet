import os

import regex
import sh
from gbdxtools import Interface

import argparse
parser = argparse.ArgumentParser()

parser.add_argument("prefix", help="Prefix of folder to be read")
parser.add_argument("output", help="Location of export folder")
parser.add_argument("--dryrun", help="Don't download, just list what will be downloaded", action='store_true')
args = parser.parse_args()

gbdx = Interface()
aws = gbdx.s3._load_info()
os.environ['AWS_ACCESS_KEY_ID'] = aws['S3_access_key']
os.environ['AWS_SECRET_ACCESS_KEY'] = aws['S3_secret_key']
os.environ['AWS_SESSION_TOKEN'] = aws['S3_session_token']


s3 = sh.aws.bake('s3')
s3_uri = "s3://{}/{}/".format(aws['bucket'], aws['prefix'])
# print(s3("ls", s3_uri))

all_folders = s3("ls", s3_uri).stdout
all_folders = regex.findall(regex.escape(args.prefix) + r'[^ ]{16}', str(all_folders))
print(all_folders)
print(len(all_folders))
if not args.dryrun:
    os.makedirs(args.output, exist_ok=True)
    for folder in all_folders:
        print(folder)
        print(s3.sync(s3_uri + folder, args.output + folder))