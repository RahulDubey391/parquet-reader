import pyarrow as pa
import pyarrow.parquet as pq
import argparse
import gcsfs
import fsspec
import os
import json
import multiprocessing
import pandas as pd

class FileExtensionError(Exception):
    pass


class MetadataCollector:
    """
    Metadata collector class for the following steps to occur:
      --List down the target files for which the metadata has to be collected - self.list_objects()
      --Collect the metadata and Write it to a target folder provided during object creation - self.collection_process() || self.write_metadata()
      --Multiprocessing step for collecting the metadata in parallel - self.run()

      NOTE: Change the set_start_method to 'forkserver' for cloud VMs and 'spawn' for local
    """
    def __init__(self, folder: str, file_system: str, bucket_name: str = None):
        self.folder = folder
        self.file_system = file_system
        self.bucket_name = bucket_name


    def list_objects(self):

        try:

            if self.file_system == 'local':
                file_list = os.listdir(self.folder)
                print('[INFO] Files found : ', file_list)
                return file_list

            if self.file_system == 'gcs':

                fs = gcsfs.GCSFileSystem()
                folder_path = f'gs://{self.bucket_name}/{self.folder}'

                file_list = fs.ls(folder_path)
                print('[INFO] Files Found', file_list)
                return file_list

        except Exception as e:
            raise(e)

    def write_metadata(self, metadata: str, file_name: str):

        try:
            if self.file_system == 'local':
                
                metadata_folder = 'metadata'
                target_metadata_folder = os.path.join(self.folder, metadata_folder)
                target_file_name = f"{file_name.split('/')[-1].split('.')[0]}.json"

                if not os.path.exists(target_metadata_folder):
                    os.mkdir(target_metadata_folder)
                
                metadata_json = json.dumps(metadata)

                if self.folder in target_file_name:
                    target_file_name = target_file_name.split(self.folder)[-1].replace('\\','')
                print(target_file_name)
                print(target_metadata_folder)
                with open(os.path.join(target_metadata_folder, target_file_name), 'w') as f:
                    f.write(metadata_json)
                    print('[INFO] Metadata file written')

            if self.file_system == 'gcs':

                fs = gcsfs.GCSFileSystem()
                metadata_json = json.dumps(metadata)
                file_path = f"gs://{self.bucket_name}/{self.folder}/metadata/{file_name.split('/')[-1].split('.')[0]}.json"
                print(file_path)
                with fs.open(file_path, 'w') as f:
                    f.write(metadata_json)
                    print('[INFO] Metadata file written')
            
            return True

        except Exception as e:
            print('[INFO] Got the exceptions : ',e)
            return False

    def collection_process(self, file):

        try:
            
            if self.file_system == 'gcs':
                file = f'gs://{file}'

            if self.file_system == 'local':
                file = os.path.join(self.folder, file)

            file_metadata = pq.read_metadata(file)
            print('[INFO] Row Groups Found : ', file_metadata.num_row_groups)

            row_group_metadata = {}
            for i in range(file_metadata.num_row_groups):
                print('[INFO] Collecting metadata for row group : ', i)
                chunk = file_metadata.row_group(i)

                column_metadata = {}
                for j in range(chunk.num_columns):
                    col = chunk.column(j).to_dict()
                    col_key = col['path_in_schema']
                    column_metadata[f'{col_key}'] = col

                row_group_metadata[f'{i}'] = column_metadata

            
            self.write_metadata(row_group_metadata, file)

            return True

        except Exception as e:
            print('[ERROR] Got the exception : ',e)
            return False
        
    def run(self):
        file_list = self.list_objects()
    
        num_cpus =  multiprocessing.cpu_count()
        print('[INFO] Available CPU Counts : ', num_cpus)
        
        multiprocessing.set_start_method('forkserver')

        processes = []
        for file in file_list:
            print('[INFO] Creating process for : ',file)
            p = multiprocessing.Process(target=self.collection_process, args=(file, ))
            processes.append(p)
            p.start()

        # Wait for all processes to finish
        for p in processes:
            print('[INFO] Joining process')
            p.join()


class MetadataProcessor:
    """
    MetadataProcessor class for processing the created metadata collection. It's better to separate out both process for metadata to be written to bucket.
      --INCLUDES:
        --Row group filtering - self.filter_row_groups()
        --Run Parallel process for filtering - self.run_filter_process()
        --Get data collected based on filtered row groups - self.getDataFrame() || self.collect_date()
    """

    def __init__(self, folder: str, file_system: str, bucket_name: str = None):
        self.folder = folder
        self.file_system = file_system
        self.bucket_name = bucket_name


    def list_objects(self):

        try:

            if self.file_system == 'local':
                file_list = os.listdir(self.folder)
                print('[INFO] Files found : ', file_list)
                return file_list

            if self.file_system == 'gcs':

                fs = gcsfs.GCSFileSystem()
                folder_path = f'gs://{self.bucket_name}/{self.folder}/metadata/'

                file_list = fs.ls(folder_path)
                print('[INFO] Files Found', file_list)
                return file_list

        except Exception as e:
            raise(e)


    def filter_row_groups(self, field_predicates: str, filtered_row_groups: dict, file: str):

        if self.file_system == 'gcs':
            
            fs = gcsfs.GCSFileSystem()
            with fs.open(f'gs://{file}', 'r') as f:
                data = f.read()

            metadata = json.loads(data)



            num_predicate = len(field_predicates.split(';'))

            predicate_dict = {i.split('=')[0]: int(i.split('=')[-1]) for i in field_predicates.split(';')}

            print('[INFO] Predicate Candidates to be searched : ', predicate_dict)

            #CHANGE THIS TO YOUR COLUMN SPECIFIC QUERY OPTIMIZER
            filtered_groups = {}
            for row_group in metadata.keys():
                if (predicate_dict['Year'] >= metadata[row_group]['Year']['statistics']['min'] and predicate_dict['Year'] <= metadata[row_group]['Year']['statistics']['max']):
                    if (predicate_dict['Month'] >= metadata[row_group]['Month']['statistics']['min'] and predicate_dict['Month'] <= metadata[row_group]['Month']['statistics']['max']):
                        filtered_groups[row_group] = metadata[row_group]

            print('[INFO] Filtered Groups Found : ', list(filtered_groups.keys()))

            filtered_row_groups[f'gs://{file}'] = filtered_groups
            return True

    def run_filter_process(self, query):

        file_list = self.list_objects()
        print('[INFO] Metadata Files Found : ', file_list)
       
        multiprocessing.set_start_method('forkserver')

        context = multiprocessing.get_context()

        manager = multiprocessing.Manager()
        filtered_row_groups = manager.dict()

        #filtered_row_groups = {}
        processes = []
        for file in file_list:
            p = multiprocessing.Process(target=self.filter_row_groups, args=(query, filtered_row_groups, file))
            processes.append(p)
            p.start()
            
        for p in processes:
            print('[INFO] Joining processes')
            p.join()
            p.close()

        output = dict(filtered_row_groups)
        print('[INFO] Number of Files for filtered row groups : ', len(output.keys()))
        print('[NFO] Consolidated Row Groups after Filtering : ', output.keys())

        return output


    def getDataFrame(self, metadata: dict, predicate_dict: dict):

        #multiprocessing.set_start_method('forkserver')
        
        context = multiprocessing.get_context()
        manager = multiprocessing.Manager()

        data_list = manager.list()

        processes = []
        for file in metadata.keys():

            p = multiprocessing.Process(target=self.collect_data, args=(data_list, file, metadata[file], predicate_dict))
            processes.append(p)
            p.start()


        for p in processes:
            print('[INFO] Joining processes')
            p.join()

        return list(data_list)


    def collect_data(self, common_list: list,  file: str, metadata: dict, predicate_dict: dict):

        try:

            if self.file_system == 'gcs':

                if '.json' or 'metadata' in file:
                    #'gs://partition-store/sample/metadata/data_10.json'

                    file = file.replace('metadata/','').replace('.json','.parquet')

                fs = gcsfs.GCSFileSystem()
                pq_file = pq.ParquetFile(file, filesystem=fs)
                df = pq_file.read_row_groups([int(i) for i in metadata.keys()]).to_pandas()
                df = df[df['Year'] == predicate_dict['Year']]
                df = df[df['Month'] == predicate_dict['Month']]
                
                common_list.append(df.to_dict())

                return True

        except Exception as e:
            raise(e)


if __name__ == '__main__':

    #Run this first if you want the metadata to be collected.
    #The final metadata will be stored per file as JSON format.
    #Metadata Location - gs://<BUCKET_NAME>/<FOLDER_NAME>/metadata
    #Data Location -gs://<BUCKET_NAME>/<FOLDER_NAME>/*.parquet
    mc = MetadataCollector('sample', 'gcs', 'partition-store')
    mc.run()

    #It's better to halt the further execution for few seconds, metadata files reflection takes few seconds before written to GCS bucekt.

    #Add a naive query predicate with ';' separated for parser to separate the predicates.
    query = 'Year=2023;Month=1'
    predicate_dict = {i.split('=')[0]: int(i.split('=')[-1]) for i in query.split(';')}

    #Row group filtering based on provided query predicates
    mp = MetadataProcessor('sample', 'gcs', 'partition-store')
    filtered_row_group_map = mp.run_filter_process(query)

    #Collect back the filtered rows into a Pandas DataFrame. 
    #Navie approach but does the job for understanding collection after predicate pushdown
    data = mp.getDataFrame(filtered_row_group_map, predicate_dict)
    print('[INFO] Data : ', len(data))

    #Write whatever format suits you, I am writing it as CSV for understanding the file without bytes to string conversion.
    pd.DataFrame(data).to_csv('gs://<BUCKET_NAME>/<FOLDER_NAME>/<FILE_NAME>.csv')
