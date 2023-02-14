import boto3
from collections import Counter

# folders = [chr(i) for i in range(48, 58)] + [chr(i) for i in range(65, 91)]
folders = ["H", "Q", "0", "D", "B", "8", "6", "G", "PJ", "11"]

# Create a dictionary with character (0-9, A-Z) as key and number as value
char_counts = {i: 0 for i in ["H", "Q", "0", "D", "B", "8", "6", "G", "PJ", "11"]}
print(char_counts)


def process_images(src_bucket, src_bucket_prefix, dest_bucket_prefix, char_counts):
    # Initialize S3 client
    s3 = boto3.client('s3')

    # For all character folders
    for folder in folders:
        bucket_prefix = src_bucket_prefix + folder + "/"
        print(f"Processing images from {bucket_prefix}...")

        # get 1000 items
        result = s3.list_objects_v2(Bucket=src_bucket, Prefix=bucket_prefix)

        # List all objects in the source bucket
        print(f"Processing {bucket_prefix}...")

        # Check if one characters have reached the target count
        if char_counts[folder] > 500:
            print(f"Character {folder} reached target amount.")
            continue

        for obj in result.get("Contents"):
            key = obj["Key"]

            try:
                try:
                    char_counts[folder] += 1
                except KeyError as e:
                    print("Error: " + str(e))
                
                # Define dest_key
                dest_key = dest_bucket_prefix + folder + "/" + key.split("/")[-1]

                # Copy the object to the destination bucket
                s3.copy_object(Bucket=src_bucket, CopySource={
                    'Bucket': src_bucket, 'Key': key}, Key=dest_key)

            except Exception as e:
                print("Error: " + str(e))

        # Check if all characters have reached the target count
        if all(count >= 500 for char, count in char_counts.items() if char not in ['I', 'O', 'Z']):
            break

        print(f"Done processing {bucket_prefix}!")
        print(char_counts)

    print(f"Done processing images from {bucket_prefix}!")
    print(char_counts)


bucket = ""
prefix = ""
dest_bucket_prefix = ""



process_images(bucket, prefix, dest_bucket_prefix, char_counts)
