import os
import time
from tiled.client.stream import Subscription
from tiled.client import from_uri
import time
import pandas
from concurrent.futures import ThreadPoolExecutor

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
    output = {"1": [0, 10, 0, 100]}
    writer_client.write_table(output)

def on_new_dataset(update: LiveChildCreated):
    "This runs when *metadata* is updated and a new dataset is created."
    print(f"New array detected: {update}")
    path_parts = tuple(update.subscription.segments)  # e.g. ('tst', 'sandbox', ...)
    METADATA_UPDATES[path_parts] = update
    sub = update.child().subscribe()
    sub.new_data.add_callback(on_the_new_segmentation)
    sub.start_in_thread(start=1)
    

def on_the_new_segmentation(update: LiveArrayData):
    "This runs when data has been uploaded for a dataset."
    # Run a segmentation on the data.
    data = update.data()  # Extract the numpy array from the update.
    # Look up the metadata which we should have already received.
    path_parts = tuple(update.subscription.segments)  # e.g. ('tst', 'sandbox', ...)
    update = METADATA_UPDATES.pop(path_parts)
    metadata = update.metadata
    executor.submit(segmentation_function, data, metadata)    

# To run the function:
if __name__ == "__main__":
    pt = from_uri(URI_IN)
    sub = pt.subscribe()
    sub.child_created.add_callback(on_new_dataset)
    print("Listening for updates. Use Ctrl+C to stop....", flush=True)
    sub.start()  # block




