from automap_hxn.loading import load_and_queue

if __name__ == "__main__":
    # This is a test script to run the load_and_queue function.
    json_path = "initial_scan_sim.json"

    load_and_queue(json_path, 392446, remote_seg=True, proceed_fine_scans=False)