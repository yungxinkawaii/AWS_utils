import boto3


def count_char(src_bucket, src_bucket_prefix):
    # Create a dictionary with character (0-9, A-Z) as key and number as value
    char_counts = {chr(i): 0 for i in range(48, 58)}
    char_counts.update({chr(i): 0 for i in range(65, 91)})
    print(char_counts)

    # Initialize S3 client
    s3 = boto3.client('s3')

    # get pages
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=src_bucket, Prefix=src_bucket_prefix)

    for result in pages:
        # List all objects in the source bucket
        for obj in result.get("Contents"):
            key = obj["Key"]

            try:
                # Extract the carplate number
                plate = key.split("_")[2]

                # Get list of unique character
                unique_char = unique_characters(plate)

                # Increment the count of each character in the carplate
                for char in unique_char:
                    try:
                        char_counts[char] += 1
                    except KeyError as e:
                        print("Error: " + str(e))

            except Exception as e:
                print("Error: " + str(e))

        print(char_counts)
    print(char_counts)


def unique_characters(string):
    unique_chars = set(string)
    return list(unique_chars)


bucket = ""
prefix = ""

count_char(bucket, prefix)
