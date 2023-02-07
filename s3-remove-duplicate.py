import boto3

s3 = boto3.client('s3')
bucket = ""


def remove_duplicates(prefix):
    # Get a list of all objects in the desired folder in your Amazon S3 bucket
    result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    files = {}
    sub_folders = set()

    # Store the file name in a dictionary where the key is the file name and the value is a list of the file's metadata
    for item in result.get('Contents'):
        key = item.get('Key')
        file_name = key.split('/')[-1]
        sub_folder_name = key.split('/')[-2]
        print(sub_folder_name)
        # print(file_name)
        # print(key)

        if file_name not in files:
            files[file_name] = True
        else:
            print(key)
            s3.delete_objects(Bucket=bucket, Key=key)
    # print(files)


remove_duplicates("")
