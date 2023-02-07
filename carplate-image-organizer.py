"""
Function: Carplate Image Organizer

Description: A Lambda function to organize images of carplates stored in an S3 bucket based on the characters in the carplate number.
Last Modified: 07/02/2023
"""

import boto3

s3 = boto3.client("s3")

# Define the desired characters to filter the images by
desired_chars = ["H", "Q", "0", "D", "B", "8", "6", "G", "PJ", "11"]

# bucket variables
bucket_name = ""
bucket_folder_prefix = ""
target_folder_prefix = ""


def lambda_handler(event, context):
    """The main function for the Lambda function."""
    # Set the batch size for processing images in chunks
    batch_size = 1000

    # Retrieve the list of images from the S3 bucket
    images = s3.list_objects(Bucket=bucket_name,
                             Prefix=bucket_folder_prefix)["Contents"]

    # Process the images in batches to reduce memory usage
    total_images = len(images)
    for i in range(0, total_images, batch_size):
        process_images(images[i:i+batch_size])

    # testing
    # images = images[:10]
    # process_images(images)


def process_images(images):
    """
    A helper function to process a batch of images.

    :param images: A list of images to process.
    """
    for image in images:
        # Only process objects with the "lp" suffix
        if image["Key"].endswith("lp.jpg"):
            # Extract the carplate number from the object's key
            carplate = image["Key"].split("_")[3]

            # Keep track of the target prefixes for each desired character
            target_prefixes = []

            # Check if the carplate number contains any of the desired characters
            for char in desired_chars:
                if char in carplate:
                    target_prefixes.append(target_folder_prefix + char + "/")

            # Copy the object to each target folder
            for target_prefix in target_prefixes:
                s3.copy_object(Bucket=bucket_name, CopySource={
                               "Bucket": bucket_name, "Key": image["Key"]}, Key=target_prefix + image["Key"].split("/")[-1])
