import boto3
import logging
from botocore.exceptions import ClientError
import time


# a file to upload to a bucket
def upload_file(s3_resource, bucket_name, file_path, file_name):
	try:
		data = open(file_path, 'rb')
		start = time.time()
		s3_resource.Bucket(bucket_name).put_object(Key=file_name, Body=data)
		end = time.time()

	except ClientError as e:
		print(e)
		logging.error(e)
		return 0

	return end-start


# function to download a file from a bucket
def download_file(s3_client, bucket_name, file_name):
	try:
		start = time.time()
		s3_client.download_file(bucket_name, file_name, file_name)
		end = time.time()

	except ClientError as e:
		print(e)
		logging.error(e)
		return 0

	return end-start


# delete a file from a bucket
def delete_file(s3_resource, bucket_name, file_name):
	try:
		s3_resource.Object(bucket_name, file_name).delete()

	except ClientError as e:
		print(e)
		logging.error(e)
		return False

	return True


# func to create a bucket
def create_bucket(s3_resource, bucket_name, region=None):
    try:
        if region is None:
            s3_resource.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            s3_resource.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
    
    except ClientError as e:
    	print(e)
    	logging.error(e)
    	return False

    return True


# func to delete a bucket
def delete_bucket(s3_client, bucket_name):
    try:
        _ = s3_client.delete_bucket(Bucket=bucket_name)
       
    except ClientError as e:
    	print(e)
    	logging.error(e)
    	return False

    return True


# func to empty a bucket
def empty_bucket(s3_resource, bucket_name):
    # Create bucket
    try:
        bucket = s3_resource.Bucket(bucket_name)
        bucket.objects.all().delete()
       
    except ClientError as e:
    	print(e)
    	logging.error(e)
    	return False

    return True


# Printing the list of buckets
def print_content(s3_client, region_name=None):
	if region_name:
		for bucket in s3_client.list_buckets()["Buckets"]:
			if s3_client.get_bucket_location(Bucket=bucket['Name'])['LocationConstraint'] == region_name:
				print(bucket["Name"])
	else:
		for bucket in s3_client.list_buckets()["Buckets"]:
			print(bucket["Name"])



# main program
if __name__ == "__main__":

	# accessing aws S3 service by creating an instance
	s3_client = boto3.client('s3')
	s3_resource = boto3.resource('s3')

	# Regions: eu-central-1(Frankfurt)  us-west-1(California)  ap-southeast-2(Sydney)

#--------------- DO some stuff here--------------

	# # create 3 buckets in 3 regions
	# _ = create_bucket(s3_resource, "testbucket-frankfurt", "eu-central-1")
	# _ = create_bucket(s3_resource, "testbucket-california", "us-west-1")
	# _ = create_bucket(s3_resource, "testbucket-sydney", "ap-southeast-2")

	# # delete the created buckets
	# _ = delete_bucket(s3_client, "testbucket-frankfurt")
	# _ = delete_bucket(s3_client, "testbucket-california")
	# _ = delete_bucket(s3_client, "testbucket-sydney")

	# # list the buckets in a specific region
	# print_content(s3_client)

# -----------------------------------------------

	bucket_name = "m7024elab1bucket1fetotasydney"

	# # uploading files
	# print("Uploading time")
	# print("1MB   -- ", upload_file(s3_resource, bucket_name, "./files/1MB.text", "1MB.text"))
	# print("10MB  -- ", upload_file(s3_resource, bucket_name, "./files/10MB.text", "10MB.text"))
	# print("100MB -- ", upload_file(s3_resource, bucket_name, "./files/100MB.text", "100MB.text"))
	# print("500MB -- ", upload_file(s3_resource, bucket_name, "./files/500MB.text", "500MB.text"))

	# # downloading files
	# print("Downloading time")
	# print("1MB   -- ", download_file(s3_client, bucket_name, "1MB.text"))
	# print("10MB  -- ", download_file(s3_client, bucket_name, "10MB.text"))
	# print("100MB -- ", download_file(s3_client, bucket_name, "100MB.text"))
	# print("500MB -- ", download_file(s3_client, bucket_name, "500MB.text"))

# -----------------------------------------------

	print("Program finished")