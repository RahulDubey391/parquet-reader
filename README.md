# parquet-reader
Naive repo for understanding read process and optimization for Apache Parquet file format

## Environment setup
```
python3.11 -m venv dataenv
source dataenv/bin/activate
pip install -r requirements.txt
```

## Run as a script
```
python main.py
```

## Run as an import
```
from main import MetadataCollector, MetadataProcessor

mc = MetadataCollector(, , ,)
mc.run()

mp = MetadataProcessor(, , ,)
mp.run_filter_process()
## Logic to run
```
## Example

This is one of the case studies I did to check the feasability of the module created. It requires some setup on your end like:
 * Data Sample Collection (I used a Python script to generte the data shown in images below)
 * Access for GCP bucket
 * VM instance with 'Acess to all Cloud APIs'

### VM Specification
I am running this code on GCP VM instance with
 * 4 vCPUs (2 Cores)
 * 16 GB Memory

### Sample Raw Data in directory "user_clicks"

I have around 20 GB of data separated out into multiple files. The characteristics of the file includes:
 * No partitiong logic - random splits
 * No ordering of the columns - predicate pushdown process most probably read all files while filtering
 * Row Group size is kept 100K records per page
 * Number of Rows per file - 1M
 * Compression Type - Snappy

NOTE : Reach out to me if you need the generation script for data.

Here's the bucket sample I am showing in the below image

<img width="418" alt="image" src="https://github.com/RahulDubey391/parquet-reader/assets/100185371/ebd8008c-16b1-444f-a9a5-5ec55b3df1d0">

### Metadata directory created after collection process
The metadata is stored in the same location as the file, it's in the folder gs://<BUCKET_NAME>/<FOLDER_NAME>/metadata

Here's the bucket sample with metadata files.

<img width="292" alt="image" src="https://github.com/RahulDubey391/parquet-reader/assets/100185371/b9240285-c3b6-43cc-b40e-9e39dc8018e3">

## Conclusion
Metadata collection, processing and filtering with data dump takes around 1 minute, which is not good enough. But it is expected since forking multiple processes has it's own over head. That's why true threading is much preferred than multiprocess communication.

## Future Scope
This is just a naive approach for reading data using Metadata which Apache Parquet provides. Future scope includes:
 * Using Bloom Filter for predicate matches
 * Better Query Optimizer and Data Serialization (Like Catalyst and Tungsten from Apache Spark)
 * Support for Sorting and merging, Coleasce or Z-ordering for file size optimization
 * Custom Partitioner support and lineage class
