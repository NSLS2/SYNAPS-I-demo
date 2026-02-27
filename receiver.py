import os
import time
from tiled.client.stream import Subscription
from tiled.client import from_uri
import time
import pandas
from concurrent.futures import ThreadPoolExecutor
from utils import *


URI_IN = os.getenv("URI_IN", "https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/synaps/reconstructions")
URI_OUT = os.getenv("URI_OUT", "https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/synaps/segmentations")

# Cache metadata updates to match them with subsequent data updates.
METADATA_UPDATES = {}

writer_client = from_uri(URI_OUT)

executor = ThreadPoolExecutor(max_workers=4)

def segmentation_function(data, metadata):
    #TODO insert here the inference call
    # output is the output of segmentation
    # structured as {"box_number": [x0, x1, y0, y1]}
    print("Doing segmentation....")
    output =  analyze_data_from_arrays(data, metadata)
    print("Writing segmentation to tiled....")
    writer_client.write_table(output, metadata)

def on_new_dataset(update):
    "This runs when *metadata* is updated and a new dataset is created."
    print(f"New array detected : {update}")
    path_parts = tuple(update.subscription.segments)  # e.g. ('tst', 'sandbox', ...)
    METADATA_UPDATES[path_parts] = update
    sub = update.child().subscribe()
    sub.child_created.add_callback(run_segmentation)
    sub.start_in_thread(max_size=100_000_000_000)
    
def run_segmentation(update):
    "This runs when data has been uploaded for a dataset."
    # Run a segmentation on the data.
    print("New Data extracted...")
    breakpoint()
    data = update.child().read()  # Extract the numpy array from the update.
    path_parts = tuple(update.subscription.segments)  # e.g. ('tst', 'sandbox', ...)
    update_parent = METADATA_UPDATES[path_parts[:-1]]
    metadata = update_parent.metadata
    executor.submit(segmentation_function, data, metadata)    



# To run the function:
if __name__ == "__main__":
    #client = from_uri('https://tiled.nsls2.bnl.gov')
    #pt = client['tst/sandbox/synaps/reconstructions']
    pt = from_uri(URI_IN)
    sub = pt.subscribe()
    sub.child_created.add_callback(on_new_dataset)
    print("Listening for updates. Use Ctrl+C to stop....", flush=True)
    sub.start()  # block

