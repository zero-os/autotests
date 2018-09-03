
## minio_perf.py

This script uses greenlets to upload/download files to/from different minios servers. 
It measures the files upload and download speeds and aggregate the results for all minios in a generated file  in this path: '/tmp/results_aggregated_{random_str}.csv' . 
Also all the logs are saved in "/var/log/j.minio_performance.log".




#### Here are the parameters this script takes

```

usage: minio_script.py [-h] -w WORKERS_NUM [-m MINIOS_NUM] -f MINIOS_FILE -p
                       [FILES_NUM_SIZES [FILES_NUM_SIZES ...]] [--no_teardown]

optional arguments:
  -h, --help            show this help message and exit
  -w WORKERS_NUM, --workers WORKERS_NUM
                        Number of greenlets to uploade/download files to/from
                        the minio server simultaneously
  -m MINIOS_NUM, --minios MINIOS_NUM
                        Number of minio servers used for running the test
  -f MINIOS_FILE, --minios_file MINIOS_FILE
                        CSV file that contains all minios, each line has a
                        minios's url, key and secret
  -p [FILES_NUM_SIZES [FILES_NUM_SIZES ...]], --files_num_sizes [FILES_NUM_SIZES [FILES_NUM_SIZES ...]]
                        pairs of the number and the size(in Bytes) of files
                        need to be generated.. ex: 10 10000000 20 1000000000:
                        this means 10 files of 10MB and 20 files of 1GB
  --no_teardown         if "--no_teardown" flag is passed, All files and
                        buckets for all minios will be removed


```
