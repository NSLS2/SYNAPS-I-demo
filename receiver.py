import os
import time
from tiled.client.stream import Subscription, LiveArrayData, LiveChildCreated
from tiled.client import from_uri
import time
import pandas
import pyarrow
from concurrent.futures import ThreadPoolExecutor
from automap_hxn.analysis import analyze_data_from_arrays


URI_IN = os.getenv("URI_IN", "https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/eugene/synaps/reconstructions")
URI_OUT = os.getenv("URI_OUT", "https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/eugene/synaps/segmentations")

# Cache metadata updates to match them with subsequent data updates.
METADATA_UPDATES = {}
SUBSCRIPTIONS = []

writer_client = from_uri(URI_OUT)

executor = ThreadPoolExecutor(max_workers=4)

# def write_table_to_tiled(client, data: dict[str, pd.DataFrame], metadata):
#     if not table := pyarrow.Table.from_pylist(data_cache)):
#         return  # Nothing to write

#     # Initialize the table and keep a reference to the client
#     df_client = client.create_appendable_table(
#         schema=schema,
#         key="internal",
#         metadata=metadata,
#         access_tags=self.access_tags,
#     )
#     self._internal_tables[desc_name] = df_client


def segmentation_function(data, metadata, path_parts):
    #TODO insert here the inference call
    # output is the output of segmentation
    # structured as {"box_number": [x0, x1, y0, y1]}
    print("Running segmentation algorithm on new data...")
    output = analyze_data_from_arrays(data, metadata)
    n_boxes = sum(len(boxes) for boxes in output.values())
    print(f"Segmentation complete. Found {n_boxes} box{'es' if n_boxes != 1 else ''}.")

    # Write the output to Tiled
    if output:
        dataset_name, table_name = path_parts
        try:
            container = writer_client[dataset_name]
        except KeyError:
             container = writer_client.create_container(dataset_name,
                                                        access_tags=["tst_sandbox"])
        
        for channel, boxes in output.items():
            if not (table := pyarrow.Table.from_pandas(boxes)):
                continue
            table_client = container.create_appendable_table(
                schema=table.schema,
                key=channel,
                metadata=metadata,
                access_tags=["tst_sandbox"],
            )
            table_client.append_partition(0, table)

    # writer_client.write_table(output, metadata)
        print("Segmentation table written to Tiled.")
    
 
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
    executor.submit(segmentation_function, data=update.data(), metadata=metadata, path_parts=path_parts[-2:])


# To run the function:
if __name__ == "__main__":
    # client = from_uri('https://tiled.nsls2.bnl.gov')
    #pt = client['tst/sandbox/synaps/reconstructions']
    pt = from_uri(URI_IN)
    sub = pt.subscribe()
    sub.child_created.add_callback(on_new_dataset)
    print("Listening for updates. Use Ctrl+C to stop....", flush=True)
    sub.start()  # block

