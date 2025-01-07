
import boto3
from os import getenv
from dotenv import load_dotenv
from re import search
from datetime import datetime

class AWS_File_Downloader():
    
    def __init__(self, bucket_name: str):
        load_dotenv()
        self.session = boto3.Session(
            aws_access_key_id= getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key= getenv("AWS_SECRET_ACCESS_KEY")
        )
        self.s3 = self.session.resource('s3')
        self.bucket = self.s3.Bucket(bucket_name)

    def get_date_time_list(self, from_time: datetime, to_time: datetime):
        date_time_list = []
        for bObj in self.bucket.objects.all():
            try:
                fileTimeStr = search('(\d+_\d+-\d+)', bObj.key).group()
                fileTime = datetime.strptime(fileTimeStr, '%d%m%Y_%H-%M')
                if(fileTime >= from_time and fileTime < to_time):
                    date_time_list.append(fileTimeStr)
            except AttributeError:
                pass

        return date_time_list
        
    def download_file(self, target_file_path: str, result_file_path: str):
        self.bucket.download_file(target_file_path, result_file_path)
    
