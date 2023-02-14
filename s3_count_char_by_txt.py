import boto3


def count_char(src_bucket, src_bucket_prefix):
    # Create a dictionary with character (0-44) as key and number as value
    char_counts = {str(i): 0 for i in range(45)}
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

            if key.endswith("txt"):

                # Download the file
                response = s3.get_object(Bucket=src_bucket, Key=key)

                # Get the contents of the file
                file_content = response["Body"].read().decode("utf-8")

                # Split the file content into lines
                lines = file_content.split("\n")

                # Extract the first word of each line
                first_words = [line.split()[0] for line in lines if line]

                # Print the list of first words
                first_words = unique_characters(first_words)

                # Increment the count of each character in the carplate
                for char in first_words:
                    try:
                        char_counts[char] += 1
                    except KeyError as e:
                        char_counts[char] = 1
        print(char_counts)


def unique_characters(string):
    unique_chars = set(string)
    return list(unique_chars)


# Call function
bucket = ""
prefix = ""

count_char(bucket, prefix)

# Postprocess
result = {}

mapping = {str(i): str(i) for i in range(10)}
mapping.update({str(i + 10): chr(i + ord('A')) for i in range(26)})
new_dict = {}
print(mapping)

for key in mapping:
    new_dict[mapping[key]] = result[key]

print(new_dict)
