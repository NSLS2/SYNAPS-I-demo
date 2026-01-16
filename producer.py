import numpy as np
import time
from tiled.client import from_uri

client = from_uri('https://tiled-staging.nsls2.bnl.gov')
pt = client['tst/sandbox/ptycho_test']

shape = (50, 50)  # array dimensions
N = 1  # number of updates
interval = 1  # delay between updates (seconds)

arr = np.random.random(shape) + 1j * np.random.random(shape)
print("Initial write... may be slow as libraries are imported for the first time.")
arr_client = pt.write_array(arr)
print(f"Created array dataset {arr_client.item['id']}")
print(arr_client)
uri = "Hello Mars!!!"
for i in range(N):
    time.sleep(interval)
    print(f"Writing update number {i}")
    new_arr = np.random.random(shape) + 1j * np.random.random(shape)
    #arr_client.write(uri, persist=False)
    arr_client.write(new_arr, persist=False)
