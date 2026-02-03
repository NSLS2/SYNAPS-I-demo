import os
import time
from tiled.client.stream import Subscription
from tiled.client import from_uri
import asyncio
from prefect.deployments import run_deployment


PREFECT_DEPLOYMENT_NAME = os.getenv("PREFECT_DEPLOYMENT_NAME", "handle-uri/hxn-als-docker")

URI_IN = os.getenv("URI_IN", "https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/synaps/reconstructions")
URI_OUT = os.getenv("URI_OUT", "https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/synaps/segmentations")

WORKFLOW_FUNCTION_ARG_NAME_IN = os.getenv("WORKFLOW_FUNCTION_ARG_NAME_IN", "uri_in")
WORKFLOW_FUNCTION_ARG_NAME_OUT = os.getenv("WORKFLOW_FUNCTION_ARG_NAME_OUT", "uri_out")

# Note that we need to have exact match of the argument names in the prefect workflow functions we call
# In case of bnl, we have our workflow function 'handle_uri' which expects 2 functions argument : 'uri_in' and 'uri_out' . Provide your function arguments name during 'podman run ...' command
# See this repo for the bnl usage reference https://github.com/NSLS2/hxn-als-workflows/blob/main/dummy.py#L6

def on_new_workflow(update):
    print(f"New array detected: {update}")
    flow_run = trigger_specific_deployment(f"{URI_IN}/{update.key}")
    print(f"Created flow run: {flow_run.name}", flush=True)
    

def trigger_specific_deployment(uri_in, uri_out=URI_OUT):
    # Trigger a deployment with a specific name and optional parameters
    flow_run = run_deployment(
        name=PREFECT_DEPLOYMENT_NAME,
        parameters={WORKFLOW_FUNCTION_ARG_NAME_IN: uri_in, WORKFLOW_FUNCTION_ARG_NAME_OUT: uri_out}
    )
    return flow_run


# To run the function:
if __name__ == "__main__":
    pt = from_uri(URI_IN)
    sub = pt.subscribe()
    sub.child_created.add_callback(on_new_workflow)
    print("Listening for updates. Use Ctrl+C to stop....", flush=True)
    sub.start()  # block




