import os
import time
from tiled.client.stream import Subscription
from tiled.client import from_uri
import time
from concurrent.futures import ThreadPoolExecutor

URI_IN = os.getenv("URI_IN", "https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/synaps/reconstructions")
URI_OUT = os.getenv("URI_OUT", "https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/synaps/segmentations")

writer_client = from_uri(URI_OUT)

executor = ThreadPoolExecutor(max_workers=4)

def segmentation_function(data):
    #TODO insert here the inference call
    output = 1 # output is the output of segmentation
    writer_client.write_array(output)

def on_new_data(update):
    print(f"New array detected: {update}")
    sub = update.child().subscribe()
    sub.new_data.add_callback(on_the_new_segmentation)
    sub.start_in_thread(start=1)
    

def on_the_new_segmentation(update):
    executor.submit(segmentation_function, update)    

# To run the function:
if __name__ == "__main__":
    pt = from_uri(URI_IN)
    sub = pt.subscribe()
    sub.child_created.add_callback(on_new_data)
    print("Listening for updates. Use Ctrl+C to stop....", flush=True)
    sub.start()  # block




