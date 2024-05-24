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
### Sample Raw Data in directory "user_clicks"
<img width="418" alt="image" src="https://github.com/RahulDubey391/parquet-reader/assets/100185371/ebd8008c-16b1-444f-a9a5-5ec55b3df1d0">

### Metadata directory created after collection process
<img width="292" alt="image" src="https://github.com/RahulDubey391/parquet-reader/assets/100185371/b9240285-c3b6-43cc-b40e-9e39dc8018e3">
