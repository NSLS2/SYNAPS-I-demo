import os
import time
from tiled.client.stream import Subscription
from tiled.client import from_uri
import asyncio
from prefect.deployments import run_deployment


PREFECT_DEPLOYMENT_NAME = os.getenv("PREFECT_DEPLOYMENT_NAME")

URI_IN_BARE = "https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/synaps/reconstructions"
URI_OUT_BARE = "https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/synaps/segmentations"

def on_new_workflow(update):
    print(f"New array detected: {update}")
    #uri_in = f"https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/synaps/reconstructions/{update.key}"
    #uri_out = f"https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/synaps/segmentations"
    
    flow_run = trigger_specific_deployment(f"{URI_IN_BARE}/{update.key}")
    print(f"Created flow run: {flow_run.name}", flush=True)
    

def trigger_specific_deployment(uri_in):
    # Trigger a deployment with a specific name and optional parameters
    flow_run = run_deployment(
        name=PREFECT_DEPLOYMENT_NAME,
        parameters={"uri_in": uri_in, "uri_out": URI_OUT_BARE}
    )
    return flow_run


# To run the function:
if __name__ == "__main__":
    client = from_uri('https://tiled.nsls2.bnl.gov')
    pt = client['tst/sandbox/synaps/reconstructions']
    sub = pt.subscribe()
    sub.child_created.add_callback(on_new_workflow)
    print("Listening for updates. Use Ctrl+C to stop....", flush=True)
    sub.start()  # block




