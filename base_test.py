from automap_hxn.loading import load_and_queue
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run load_and_queue function with specified parameters.")
    parser.add_argument("--json_path", type=str, default="initial_scan_sim.json", help="Path to the JSON configuration file.")
    parser.add_argument("--scan_id", type=int, default=392456, help="Scan ID to process.")
    parser.add_argument('-r', "--remote_seg", action="store_true", help="Enable remote segmentation.")
    parser.add_argument("--proceed_fine_scans", action="store_true", help="Proceed with fine scans.")

    args = parser.parse_args()

    load_and_queue(args.json_path, args.scan_id, remote_seg=args.remote_seg, proceed_fine_scans=args.proceed_fine_scans)