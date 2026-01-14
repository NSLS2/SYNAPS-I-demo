# SYNAPS-I-demo

## Demo overview

1. HXN writes a dataset for segmentation to `tiled.nsls2.bnl.gov`.
2. A long-running container on the ALS Compute cluster subscribes to
   `tiled.nsls2.bnl.gov`, listening for updates about new datasets
   ready to be segmented.
3. When it an update is received, the long-running container
   schedules a Prefect job to run on the ALS Compute cluster, passing
   in the URL to the dataset of interest.
4. The Prefect job downloads the dataset to be segmented, segments it (AI!),
   and uploads the results to `tiled.nsls2.bnl.gov`
5. HXN subscribes to `tiled.nsls2.bnl.gov` to be notified when a segmentation
   result has been uploaded. It accessed it, and proceeds with its existing
   workflow.

## This repository

This repository will provide a configurable skeleton that our partners at ALS
can quickly plug their AI segmentation backend into.

Specifically: a container that subscribes to a (configurable) Tiled node, listening for
updates over WebSockets and schedules a (configurable) Prefect Flow to run
when an update is received. It should pass the URL to the dataset of
interest as a parameter to the Flow.
