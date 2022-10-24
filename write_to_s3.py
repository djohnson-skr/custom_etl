from distutils.command.upload import upload
import boto3 
import os
from botocore.exceptions import ClientError
import logging
import re

def write_to_s3(file_dir, bucket_name):
    file_match_counter = 0
    client = boto3.client('s3',
                            aws_access_key_id='AKIAUUSGHFEUAWFBD6NR',
                            aws_secret_access_key='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'
    )

    for file in os.listdir(file_dir):
        if re.search('.csv$', file):
            local_file_path = file_dir + file
            bucket = bucket_name
            s3_file_path = 'python_data/' + str(file)
            try:
                client.upload_file(local_file_path, bucket, s3_file_path)
                file_match_counter =+ 1
                os.rename(local_file_path, local_file_path + "_OK")
                print('uploaded file')
            except ClientError as e:
                logging.error(e)
                print("There was an issue uploading the file")

    print("Uploaded " + str(file_match_counter) + " files")
