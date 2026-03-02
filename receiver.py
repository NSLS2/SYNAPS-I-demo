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

def segmentation_function(data, metadata, path_parts):
    """Run the segmentation algorithm on the new data and write the results back to Tiled.
    
    Args:
        data: The numpy array data from the new Tiled array.
        metadata: The metadata dictionary associated with the parent dataset.
        path_parts: Tuple of (dataset_name, table_name) for writing results.
    """

    #The output of segmentation isstructured as {"channel": ['label', 'cx', 'cy', 'num_x', 'num_y']}
    print("Running segmentation algorithm on new data...")
    output = analyze_data_from_arrays(data, metadata)

    # #### Uncomment for testing -- results from the local run
    # output = {'Ni': pandas.DataFrame({
    #     'label': ['Individual Blob Ni #1', 'Individual Blob Ni #2', 'Individual Blob Ni #3', 'Individual Blob Ni #4'],
    #     'cx': [1.20064, 5.10064, -5.39936, 1.10064],
    #     'cy': [-3.40246, 1.69754, 4.49754, 5.19754],
    #     'num_x': [8.0, 8.6, 10.2, 7.0],
    #     'num_y': [8.0, 8.6, 10.2, 7.0]}
    #     ),
    #     'Mn': pandas.DataFrame({
    #         'label': ['Individual Blob Mn #1', 'Individual Blob Mn #2', 'Individual Blob Mn #3', 'Individual Blob Mn #4'],
    #         'cx': [0.70064, 5.60064, -6.39936, -5.39936],
    #         'cy': [-3.20246, 1.79754, 3.19754, 7.69754],
    #         'num_x': [9.0, 8.8, 7.4, 6.2],
    #         'num_y': [9.0, 8.8, 7.4, 6.2]}
    #     ),
    #     'NiCoMn': pandas.DataFrame({
    #         'label': ['Union Box NiCoMn #1'],
    #         'cx': [-0.19936],
    #         'cy': [0.42254],
    #         'num_x': [22.4],
    #         'num_y': [22.65]}
    #     )
    # }

    n_boxes = sum(len(boxes) for boxes in output.values())
    print(f"Segmentation complete. Found {n_boxes} box{'es' if n_boxes != 1 else ''}.")

    # Write the output to Tiled
    if output:
        dataset_name, _ = path_parts[-2:]
        try:
            container = writer_client[dataset_name]
        except KeyError:
             container = writer_client.create_container(dataset_name,
                                                        access_tags=["tst_sandbox"])
        
        for channel, boxes in output.items():
            try:
                if not (table := pyarrow.Table.from_pandas(boxes)):
                    continue
                table_client = container.create_appendable_table(
                    schema=table.schema,
                    key=channel,
                    metadata=metadata,
                    access_tags=["tst_sandbox"],
                )
                table_client.append_partition(0, table)
            except Exception as e:
                print(f"Failed to write table for channel {channel}: {e}")
                import traceback
                traceback.print_exc()

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
    client = from_uri(URI_IN)
    sub = client.subscribe()
    sub.child_created.add_callback(on_new_dataset)
    print("Listening for updates. Use Ctrl+C to stop....", flush=True)
    sub.start()  # block

