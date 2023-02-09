import boto3

# folders = [chr(i) for i in range(48, 58)] + [chr(i) for i in range(65, 91)]
folders = ['P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']


def process_images(src_bucket, src_bucket_prefix, dest_bucket_prefix, char_counts):
    # Create a dictionary with character (0-9, A-Z) as key and number as value
    # char_counts = {chr(i): 0 for i in range(48, 58)}
    # char_counts.update({chr(i): 0 for i in range(65, 91)})
    # print(char_counts)

    # Initialize S3 client
    s3 = boto3.client('s3')

    # For all character folders
    for folder in folders:
        bucket_prefix = src_bucket_prefix + folder + "/"
        print(f"Processing images from {bucket_prefix}...")

        # List all objects in the source bucket
        result = s3.list_objects_v2(Bucket=src_bucket, Prefix=bucket_prefix)

        for obj in result.get("Contents"):
            key = obj["Key"]

            try:
                # Extract the carplate number
                plate = key.split("_")[2]

                # Increment the count of each character in the carplate
                for char in plate:
                    try:
                        char_counts[char] += 1
                    except KeyError as e:
                        print("Error: " + str(e))

                # Copy the object to the destination bucket
                image_name = key.split("/")[-1]

                copy_source = {'Bucket': src_bucket, 'Key': key}
                s3.copy_object(Bucket=src_bucket,
                               CopySource=copy_source, Key=dest_bucket_prefix + image_name)
            except Exception as e:
                print("Error: " + str(e))

        # Check if all characters have reached the target count
        if all(count >= 4900 for char, count in char_counts.items() if char not in ['I', 'O', 'Z']):
            break

        print(char_counts)


bucket = ""
prefix = ""
dest_bucket_prefix = ''
char_counts = {}

process_images(bucket, prefix, dest_bucket_prefix, char_counts)
