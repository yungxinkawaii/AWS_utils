import boto3

# Create a mapping from number to word
mapping = {str(i): str(i) for i in range(10)}
mapping.update({str(i + 10): chr(i + ord('A')) for i in range(26)})
print(mapping)

# Target characters
desired_chars = ["H", "Q", "0", "D", "B", "8", "6", "G", "P", "J"]

# Create a dictionary with character (0-9, A-Z) as key and number as value
char_counts = {i: 0 for i in desired_chars}
print(char_counts)


def filter_v3(src_bucket, src_bucket_prefix, dest_bucket, dest_bucket_prefix):
    # Initialize S3 client
    s3 = boto3.client('s3')

    # Copy desired char
    desired_chars_copy = desired_chars.copy()

    # get pages
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=src_bucket, Prefix=src_bucket_prefix)

    for result in pages:
        # Exit while reach 500 for all
        if all(count >= 1000 for char, count in char_counts.items()):
            break

        # List all objects in the source bucket
        for obj in result.get("Contents"):
            key = obj["Key"]

            if key.endswith("txt"):
                try:
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

                    # Convert to characters
                    characters = [mapping[word] for word in first_words]

                    # Get only the target characters
                    target_chars = [
                        char for char in characters if char in desired_chars_copy]

                    # Increment the count of each character in the carplate
                    if target_chars:
                        assigned_char = min(
                            target_chars, key=lambda x: char_counts[x])
                        char_counts[assigned_char] += 1
                        print(assigned_char)

                        # source and destination key
                        source_key = key[:-3] + "jpg"
                        dest_key = dest_bucket_prefix + assigned_char + \
                            "/" + source_key.split("/")[-1]

                        # Copy the object to the destination bucket
                        s3.copy_object(Bucket=dest_bucket, CopySource={
                                       'Bucket': src_bucket, 'Key': source_key}, Key=dest_key)
                        print(dest_key)
                    
                    # Remove char from desired_chars_copy when reaches 520
                    if char_counts[assigned_char] >= 1000:
                        desired_chars_copy.remove(assigned_char)

                    # Exit while reach 10K for all
                    if sum(char_counts.values()) >= 10000:
                        break

                except Exception as e:
                    print("Error: " + str(e))

        print(char_counts)

# Get the unique characters only
def unique_characters(string):
    unique_chars = set(string)
    return list(unique_chars)


# Call functions
bucket = ""
prefix = ""

dest_bucket = ""
dest_bucket_prefix = ""

filter_v3(bucket, prefix, dest_bucket, dest_bucket_prefix)
