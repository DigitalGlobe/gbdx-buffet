# GBDX Buffet
This is where we get all you can eat imagery.

# Setup

All operations on GBDX require credentials. You can sign up for a
GBDX account at https://gbdx.geobigdata.io. Your GBDX credentials
are found under your account profile.
gbdxtools expects a config file to exist at ~/.gbdx-config with
your credentials. (See formatting for this 
file here: https://github.com/tdg-platform/gbdx-auth#ini-file.)

For a quick installation with virtualenv, install can be directly from github

```bash
virtualenv /path/to/env -p python3
source /path/to/env/bin/activate
pip install git+git://github.com/DigitalGlobe/gbdx-buffet#egg=gbdx-buffet
```
