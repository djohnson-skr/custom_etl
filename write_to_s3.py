from distutils.command.upload import upload
import boto3 
import os
from botocore.exceptions import ClientError
import logging
import re
from keys import aws_access_key_id
from keys import aws_secret_access_key

def write_to_s3(file_dir, bucket_name):
    file_match_counter = 0
    client = boto3.client('s3',
                            aws_access_key_id= aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key
    )

    for file in os.listdir(file_dir):
        if re.search('.csv$', file):
            local_file_path = file_dir + file
            bucket = bucket_name
            s3_file_path = 'python_data/' + str(file)
            try:
                client.upload_file(local_file_path, bucket, s3_file_path)
                file_match_counter += 1
                os.rename(local_file_path, local_file_path + "_OK")
                print('uploaded file')
            except ClientError as e:
                logging.error(e)
                print("There was an issue uploading the file")

    print("Uploaded " + str(file_match_counter) + " files")
