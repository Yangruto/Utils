import os
import json
from google.cloud import storage
from googleapiclient.discovery import build

class GCS:
    def __init__(self, key_path:str, project_id:str):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path
        self.project_id = project_id
        self.client = storage.Client(project=self.project_id)

    def upload_bucket(self, bucket_name:str, source_file_name:str, destination_blob_name:str):
        """
        Upload a file to the bucket
            bucket_name:
            suource_file_name:
            destination_blob_name:
        """
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(
            data = json.dumps(source_file_name),
            content_type = 'application/json'
        )
        print(f'File has saved as {destination_blob_name}.')

    def check_blob(self, bucket_name:str, blob_name:str):
        """
        Check blob is exist or not
            bucke_name: bucket name
            blob_name: blob name
        """
        bucket = self.client.get_bucket(bucket_name)
        tmp_blob = bucket.blob(blob_name)
        return tmp_blob.exists()

    def create_folder(self, bucket_name:str, folder_name:str):
        """
        Create folder, name must end with /
            bucket_name: bucket name
            folder_name: folder name
        """
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(folder_name)
        blob.upload_from_string('')

    def get_drive_file(self, id):
        '''
        Get all files' name and id from specific drive or folder id.
            Input: google drive/folder id
            Output: list of dict of files
        '''
        service = build('drive', 'v3')
        resp = service.files().list(q=f"'{id}' in parents").execute()
        file_list = resp.get('files')
        return file_list

