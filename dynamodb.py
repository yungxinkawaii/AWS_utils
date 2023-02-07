from boto3.dynamodb.conditions import Attr
import boto3
import botocore
import json
import os
from decimal import Decimal
from datetime import datetime
import time
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


DB_TABLE_NAME = ""


class File:
    """Encapsulates an Amazon DynamoDB table of file data."""

    def __init__(self, dyn_resource, tablename):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource
        self.table = self.dyn_resource.Table(tablename)

    def create_table(self, table_name):
        """
        Creates an Amazon DynamoDB table that can be used to store file data.
        The table uses the hostname as the partition key and the
        timestamp as the sort key.

        :param table_name: The name of the table to create.
        :return: The newly created table.
        """
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": "hostname",
                        "KeyType": "HASH"},  # Partition key
                    {"AttributeName": "version", "KeyType": "RANGE"},  # Sort key
                ],
                AttributeDefinitions=[
                    {"AttributeName": "hostname", "AttributeType": "S"},
                    {"AttributeName": "version", "AttributeType": "N"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            self.table.wait_until_exists()
        except botocore.exceptions.ClientError as err:
            print(
                "Couldn't create table %s. Here's why: %s: %s",
                table_name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            print("table created")
            return self.table

    def add_file(self, hostname, timestamp, file):
        """
        Adds a file to the table.

        :param hostname: The hostname.
        :param timestamp: The timestamp.
        :param file: The file pass in.
        """
        data = {
            "hostname": hostname,
            "time_stamp": timestamp,
            "file": file,
            "version": self.version_incrementor(hostname),
        }
        data = json.loads(json.dumps(data), parse_float=Decimal)

        try:
            self.table.put_item(Item=data)
        except botocore.exceptions.ClientError as err:
            print(
                "Couldn't add file %s to table %s. Here's why: %s: %s",
                hostname,
                self.table.name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    def version_incrementor(self, hostname):
        """
        Incrementor for version number.
        Query the latest version of the hostname and increment it.

        :param hostname: The hostname.
        :return: The incremented version number.
        """
        response = self.table.scan(
            FilterExpression=Attr("hostname").eq(hostname))
        return len(response["Items"]) + 1


def publish_file(file):
    """Publish the latest file version to DyanamoDB."""
    # get hostname from .env
    hostname = os.getenv("HOSTNAME")

    # set up db connection & session
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_Access_Key_ID"),
        aws_secret_access_key=os.getenv("AWS_Secret_Access_Key"),
        region_name=os.getenv("AWS_Region_Name"),
    )

    dynamodb = session.resource("dynamodb")

    # publish data to db
    db = File(dynamodb, DB_TABLE_NAME)
    db.add_file(hostname, str(datetime.now()), file)


async def publish_file_async(retry=3, wait=5):
    """Async function that can be called from update_file.py.

    Args:
        retry (int, optional): Retry times. Defaults to 3.
        wait (int, optional): Time to wait before the next retry. Defaults to 5.
    """
    count = 0
    while count < retry:
        try:
            publish_file()
            logger.info(
                f"[file_db] Successfully updated new file onto DynamoDB")
            return

        # catch error
        except Exception:
            count += 1
            time.sleep(wait)
            continue

    logger.error(
        f"[file_db] Failed to publish latest file version to DynamoDB")
