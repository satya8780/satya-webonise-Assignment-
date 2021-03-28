import boto3
import time
import sys

# todays\'s epoch
_tday = time.time()
duration = 86400*180 #180 days in epoch seconds
#checkpoint for deletion
_expire_limit = tday-duration
# initialize s3 client
s3_client = boto3.client('s3')
my_bucket = "my-s3-bucket"
my_ftp_key = "my-s3-key/"
_file_size = [] #just to keep track of the total savings in storage size

#Functions
#works to only get us key/file information
def get_key_info(bucket="my-s3-bucket", prefix="my-s3-key/"):

    print(f"Getting S3 Key Name, Size and LastModified from the Bucket: {bucket} with Prefix: {prefix}")

    key_names = []
    file_timestamp = []
    file_size = []
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    while True:
        response = s3_client.list_objects_v2(**kwargs)
        for obj in response["Contents"]:
            # exclude directories/folder from results. Remove this if folders are to be removed too
            if "." in obj["Key"]:
                key_names.append(obj["Key"])
                file_timestamp.append(obj["LastModified"].timestamp())
                file_size.append(obj["Size"])
        try:
            kwargs["ContinuationToken"] = response["NextContinuationToken"]
        except KeyError:
            break

    key_info = {
        "key_path": key_names,
        "timestamp": file_timestamp,
        "size": file_size
    }
    print(f'All Keys in {bucket} with {prefix} Prefix found!')

    return key_info


# Check if date passed is older than date limit
def _check_expiration(key_date=_tday, limit=_expire_limit):
    if key_date < limit:
        return True


# connect to s3 and delete the file
def delete_s3_file(file_path, bucket=my_bucket):
    print(f"Deleting {file_path}")
    s3_client.delete_object(Bucket=bucket, Key=file_path)
    return True


