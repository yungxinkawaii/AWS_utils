import boto3
from collections import Counter

# folders = [chr(i) for i in range(48, 58)] + [chr(i) for i in range(65, 91)]
folders = ["R"]


def process_images(src_bucket, src_bucket_prefix, dest_bucket_prefix, char_counts):
    # Initialize S3 client
    s3 = boto3.client('s3')

    # For all character folders
    for folder in folders:
        bucket_prefix = src_bucket_prefix + folder + "/"
        print(f"Processing images from {bucket_prefix}...")

        # get pages
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=src_bucket, Prefix=bucket_prefix)

        # List all objects in the source bucket
        for result in pages:
            print(f"Processing a page from {bucket_prefix}...")

            # Check if one characters have reached the target count
            if char_counts[folder] > 4900:
                print(f"Character {folder} reached target amount.")
                break

            for obj in result.get("Contents"):
                key = obj["Key"]

                try:
                    # Check if the image already exists in the destination folder
                    dest_key = dest_bucket_prefix + key.split("/")[-1]
                    result = s3.list_objects_v2(
                        Bucket=src_bucket, Prefix=dest_key)
                    if "Contents" in result:
                        contents = result["Contents"]
                        if len(contents) > 0:
                            continue

                    # Extract the carplate number
                    plate = key.split("_")[2]

                    # Get unique char
                    unique_char = unique_characters(plate)

                    # Increment the count of each character in the carplate
                    for char in unique_char:
                        try:
                            char_counts[char] += 1
                        except KeyError as e:
                            print("Error: " + str(e))

                    # Copy the object to the destination bucket
                    s3.copy_object(Bucket=src_bucket, CopySource={
                        'Bucket': src_bucket, 'Key': key}, Key=dest_key)
                except Exception as e:
                    print("Error: " + str(e))

            # Check if all characters have reached the target count
            if all(count >= 4900 for char, count in char_counts.items() if char not in ['I', 'O', 'Z']):
                break

            print(f"Done processing a page from {bucket_prefix}!")
            print(char_counts)

        print(f"Done processing images from {bucket_prefix}!")
        print(char_counts)


def unique_characters(string):
    unique_chars = set(string)
    return list(unique_chars)


bucket = ""
prefix = ""
dest_bucket_prefix = ''
char_counts = {}

process_images(bucket, prefix, dest_bucket_prefix, char_counts)
