import os
import boto3

# AWS S3 config
aws_access_key_id = ''
aws_secret_access_key = ''
region_name = ''
bucket_name =''
os.environ['HTTPS_PROXY'] = ''

class AWS:
    def __init__(self, region_name:str, aws_access_key_id:str, aws_secret_access_key:str) -> object:
        """
        Connect to AWS.
            region_name: AWS server region
            aws_access_key_id: access key
            aws_secret_access_key: secret access key
        """
        self.client = boto3.client(service_name='s3',
            region_name = region_name,
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key)

    def set_bucket(self, bucket_name:str) -> None:
        """
        Set bucket name.
            bucket_name: bucket name
        """
        self.bucket_name = bucket_name

    def get_aws_file(self, file_path:str) -> None:
        """
        Get an AWS file(Json).
            file_path: file Path in AWS
        """
        response = self.client.get_object(Bucket=self.bucket_name, Key=file_path)
        self.data = json.loads(response['Body'].read())
        self.data_name = file_path.split('/')[-1]
        self.key_list = []
        self.get_keys(self.data)

    def get_aws_folder_file_list(self, folder_path:str) -> list:
        """
        Get all files from a specific folder.
            folder_path: folder path
        """
        file_list = []
        paginator = self.client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name, Delimiter='/', Prefix=folder_path)
        for page in pages:
            for obj in page['Contents']:
                if 'Key' in obj.keys():
                    file_list.append(obj['Key'])
        return file_list

    def get_aws_folder_list(self) -> list:
        """
        Get all folders.
        """
        folder_list = []
        def get_folder(prefix):
            response = self.client.list_objects(Bucket=self.bucket_name, Delimiter='/', Prefix=prefix)
            if 'CommonPrefixes' in response.keys():
                [folder_list.append(i['Prefix']) for i in response['CommonPrefixes']]
                return [get_folder(i['Prefix']) for i in response['CommonPrefixes']]
            else:
                return None
        get_folder('')
        return folder_list

    def get_aws_all_file_list(self) -> list:
        """
        Get all files of the bucket.
        """
        folder_list = ['/']
        folder_list += self.get_aws_folder_list()
        file_list = []
        for folder_path in folder_list:
            sub_file_list = self.get_aws_folder_file_list(folder_path)
            file_list += sub_file_list
        return file_list

    def read_json_file(self, path:str) -> None:
        """
        Read a JSON file.
            file: file path
        """
        with open(path, 'r') as reader:
            self.data_name = path.split('/')[-1]
            self.data = json.loads(reader.read())
            print(f'Read {self.data_name}')

    def get_keys(self, data) -> None:
        """
        Get all JSON keys. (Parse the nested structure of json)
        Save in key_list
            data: json data
        """
        if type(data) == dict:
            for i in data:
                if type(data[i]) == dict:
                    self.key_list.append(i)
                    self.get_keys(data[i])
                elif type(data[i]) == list:
                    self.key_list.append(i)
                    self.get_keys(data[i])
                else:
                    self.key_list.append(i)
        elif type(data) == list:
            for i in data:
                if type(i) == dict:
                    self.get_keys(i)
                elif type(i) == list:
                    self.get_keys(i)