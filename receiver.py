import time
from tiled.client.stream import Subscription
from tiled.client import from_uri
import asyncio
from prefect.deployments import run_deployment

def on_new_array(update):
    print(f"New array detected: {update.key}")
    sub = update.child().subscribe()
    sub.new_data.add_callback(on_array_data)
    # Start from the first update.
    sub.start_in_thread(start=1)


def trigger_specific_deployment(uri):
    # Trigger a deployment with a specific name and optional parameters
    flow_run = run_deployment(
        name="handle-uri/hxn-als-docker",
        parameters={"uri": uri}
    )
    return flow_run

def on_array_data(update):
    print(update.data())
    print("Received something")
    uri = "Hello Mars"
    flow_run = trigger_specific_deployment(uri)
    print(f"Created flow run: {flow_run.name}")


# To run the function:
if __name__ == "__main__":
    client = from_uri('https://tiled-staging.nsls2.bnl.gov')
    pt = client['tst/sandbox/ptycho_test']
    sub = pt.subscribe()
    sub.child_created.add_callback(on_new_array)
    print("Listening for updates. Use Ctrl+C to stop....")
    sub.start()  # block




