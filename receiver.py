import os
import time
from tiled.client.stream import Subscription, LiveArrayData, LiveChildCreated
from tiled.client import from_uri
import time
import pandas
from concurrent.futures import ThreadPoolExecutor
from automap_hxn.analysis import analyze_data_from_arrays


URI_IN = os.getenv("URI_IN", "https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/eugene/synaps/reconstructions")
URI_OUT = os.getenv("URI_OUT", "https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/eugene/synaps/segmentations")

# Cache metadata updates to match them with subsequent data updates.
METADATA_UPDATES = {}
SUBSCRIPTIONS = []

writer_client = from_uri(URI_OUT)

executor = ThreadPoolExecutor(max_workers=4)

def segmentation_function(data, metadata):
    #TODO insert here the inference call
    # output is the output of segmentation
    # structured as {"box_number": [x0, x1, y0, y1]}
    print("Doing segmentation....")
    output = analyze_data_from_arrays(data, metadata)
    print("Writing segmentation to tiled....")
    breakpoint()
    writer_client.write_table(output, metadata)
    
 
def on_new_dataset(update: LiveChildCreated):
    "This runs when a new dataset is created in the root container."
    path_parts = tuple(update.subscription.segments) + (update.key,)
    print(f"New dataset created: {'/'.join(path_parts)}")
    METADATA_UPDATES[path_parts] = update.metadata  # Cache the metadata for later use
    sub = update.child().subscribe()
    sub.child_created.add_callback(on_new_array)
    sub.start_in_thread()
    SUBSCRIPTIONS.append(sub)


def on_new_array(update: LiveChildCreated):
    "This runs when a new array is created in the container; may not have any data yet!"
    print(f"New array created: {update.key}. Waiting for data to be uploaded...")
    sub = update.child().subscribe()  # subscribe to the array to get data updates
    sub.new_data.add_callback(run_segmentation)
    sub.start_in_thread(start=0, max_size=100_000_000_000)  # large max_size for bigger images
    SUBSCRIPTIONS.append(sub)


def run_segmentation(update: LiveArrayData):
    "This runs when data is uploaded to the array. The metadata is retrieved from "
    "the cache and passed to the segmentation function."
    path_parts = tuple(update.subscription.segments)
    metadata = METADATA_UPDATES.get(path_parts[:-1], {})  # Get metadata for the parent dataset
    executor.submit(segmentation_function, data=update.data(), metadata=metadata)   


# To run the function:
if __name__ == "__main__":
    # client = from_uri('https://tiled.nsls2.bnl.gov')
    #pt = client['tst/sandbox/synaps/reconstructions']
    pt = from_uri(URI_IN)
    sub = pt.subscribe()
    sub.child_created.add_callback(on_new_dataset)
    print("Listening for updates. Use Ctrl+C to stop....", flush=True)
    sub.start()  # block

