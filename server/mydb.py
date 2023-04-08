
import logging
from botocore.exceptions import ClientError

import sys    
print("In module products sys.path[0], __package__ ==", sys.path[0], __package__)


logger = logging.getLogger(__name__)


MAX_GET_SIZE = 100  # Amazon DynamoDB rejects a get batch larger than 100 items.

def myprint(nn):
    print("myabs-myprint:"+nn)

def myabsf(n):
        if n < 0:
            return -n
        else:
            return n

# create table
class BaseDB:
    """Encapsulates an Amazon DynamoDB table of movie data."""
    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource
        self.table = None
        print(type(dyn_resource))

    def create_table(self, table_name):
        """
        Creates an Amazon DynamoDB table that can be used to store movie data.
        The table uses the release year of the movie as the partition key and the
        title as the sort key.

        :param table_name: The name of the table to create.
        :return: The newly created table.
        """
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'year', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'title', 'KeyType': 'RANGE'}  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'year', 'AttributeType': 'N'},
                    {'AttributeName': 'title', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})
            self.table.wait_until_exists()
        except self.dyn_resource.exceptions.ResourceInUseException as e:
            logger.error(f"引发异常：{e.toString()}")
            print("引发异常：",repr(e))
            raise
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        finally:
            print(f"Create {type(self.table)} table {table_name}success")
            return self.table
        
    def select_table(self):
        #self.dyn_resource.select
        return None
        
    def delete_table(self):
        """
        Deletes the table.
        """
        try:
            self.table.delete()
            self.table = None
        except ClientError as err:
            logger.error(
                "Couldn't delete table. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        finally:
            print(f"delete_table: {self.table.TableName}")

    def insert_item(self,data):
        try:
            print(f"insert_item to table: {data}")
            self.table.put_item(data)
        except ClientError as err:
            print(
                "Couldn't delete table. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            print("insert success")
            
    def insert_set(self,dataset):
        try:
            with self.table.batch_writer(overwrite_by_pkeys=['year', 'title']) as batch:
                for item in dataset:
                    batch.put_item( item )
        except ClientError as err:
            print(
                "Couldn't delete table. Here's why: %s: %s",
                    err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            print("insert success")
    
    def delete_item(self, title, year):
        """
        Deletes a movie from the table.

        :param title: The title of the movie to delete.
        :param year: The release year of the movie to delete.
        """
        try:
            self.table.delete_item(Key={'year': year, 'title': title})
        except ClientError as err:
            logger.error(
                "Couldn't delete movie %s. Here's why: %s: %s", title,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

