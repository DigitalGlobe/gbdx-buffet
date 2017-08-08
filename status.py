from logging import getLogger

from gbdxtools import Interface
from gbdxtools.workflow import Workflow as WorkflowAPI

log = getLogger()
log.setLevel('DEBUG')

gbdx = Interface()

workflow_api = WorkflowAPI()


def main():
    resp = gbdx.gbdx_connection.get("https://geobigdata.io/workflows/v1/workflows")

    for w in resp.json()['Workflows']:
        print(workflow_api.get(w))

if __name__ == '__main__':
    main()
