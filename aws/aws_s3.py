import gzip
import pytz
import boto3
import logging
import pandas as pd
from io import BytesIO
from io import TextIOWrapper
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s  File:%(filename)s  Function:%(funcName)s()  Line:%(lineno)d  Msg: %(message)s'
)


class S3:
    def __init__(self, **kwargs):
        """
        This accepts aws_access_key_id and aws_secret_access_key as keyword arguments.
        No need to provide credentials if the aws cli is configured with aws keys.
        """
        if all(field in kwargs for field in ('aws_access_key_id', 'aws_secret_access_key')):
            self.credentials = {
                'aws_access_key_id': kwargs['aws_access_key_id'],
                'aws_secret_access_key': kwargs['aws_secret_access_key']
            }
        else:
            self.credentials = None

        self.logger = self._get_logger()
        self.s3_client = self._get_client()
        self.s3_resource = self._get_resource()

    def _get_client(self):
        if self.credentials:
            return boto3.client('s3', **self.credentials)
        else:
            return boto3.client('s3')

    def _get_resource(self):
        if self.credentials:
            session = boto3.session.Session(**self.credentials)
            return session.resource('s3')
        else:
            return boto3.resource('s3')

    def _get_logger(self):
        logger = logging.getLogger(self.__str__())
        return logger

    def __str__(self):
        # TODO implement fancy way to express the object name
        return 's3 object'

    def _get_matching_objects(self, bucket_name, prefix='', suffix=''):
        """
        Generate objects in an S3 bucket.
        :param prefix: Only fetch objects whose key starts with this prefix (optional).
        :param suffix: Only fetch objects whose keys end with this suffix (optional).
        """

        kwargs = {'Bucket': bucket_name}

        # If the prefix is a single string (not a tuple of strings), we can
        # do the filtering directly in the S3 API.
        if isinstance(prefix, str):
            kwargs['Prefix'] = prefix

        while True:
            # The S3 API response is a large blob of metadata.
            # 'Contents' contains information about the listed objects.
            resp = self.s3_client.list_objects_v2(**kwargs)

            try:
                contents = resp['Contents']
            except KeyError:
                return

            for obj in contents:
                key = obj['Key']
                if key.startswith(prefix) and key.endswith(suffix):
                    yield obj
            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until we
            # reach the final page (when this field is missing).
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def get_matching_keys(self, bucket_name, prefix='', suffix='', limit=0):
        """
        Generate the keys in an S3 bucket.
        :param bucket_name: Name of bucket (str)
        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        :param limit: Only fetch this many keys
        """
        key_list = []
        for obj in self._get_matching_objects(bucket_name=bucket_name, prefix=prefix, suffix=suffix):
            key_list.append(obj['Key'])

            if len(key_list) == limit:
                break

        return key_list

    def get_oldest_key(self, bucket_name, prefix='', suffix=''):
        """
        :param bucket_name: bucket name (str)
        :param prefix: prefix of the key to check (str)
        :param suffix: suffix of the key to check (str)
        :return: tuple(oldest_key, last_modified_on)
        """
        object_iterator = self._get_matching_objects(bucket_name=bucket_name, prefix=prefix, suffix=suffix)
        oldest_file = None
        oldest_date = datetime.now(tz=pytz.timezone('utc'))
        for content in object_iterator:
            modified_date = content["LastModified"]
            if content["Key"] != prefix and modified_date < oldest_date:
                oldest_date = modified_date
                oldest_file = content["Key"]
        return oldest_file, oldest_date

    def get_latest_key(self, bucket_name,  prefix='', suffix=''):
        """
        :param bucket_name: bucket name (str)
        :param prefix: prefix of the key to check (str)
        :param suffix: suffix of the key to check (str)
        :return: tuple (latest_key, last_modified_on)
        """
        object_iterator = self._get_matching_objects(bucket_name=bucket_name, prefix=prefix, suffix=suffix)
        latest_file = None
        latest_date = datetime(1900, 1, 1, tzinfo=pytz.timezone('utc'))
        for content in object_iterator:
            last_modified = content["LastModified"]
            if content["Key"] != prefix and last_modified > latest_date:
                latest_date = last_modified
                latest_file = content["Key"]
        return latest_file, latest_date

    def is_file_present(self, bucket_name, folder='', file=''):
        """
        Checks if a key(file) exist in a folder
        :param folder: folder inside the bucket
        :param file: file inside the folder
        """
        for obj in self.get_matching_keys(bucket_name=bucket_name, prefix=folder):
            if obj == folder+"/"+file:
                return True
        return False

    def is_folder_present(self, bucket_name, prefix=None):
        """
        Checks if a key(folder) exist in a folder
        :param bucket_name: bucket inside the bucket
        :param prefix: folder/subfolder inside the bucket
        """
        for obj in self.get_matching_keys(bucket_name=bucket_name, prefix=prefix):
            if obj.startswith(prefix+"/"):
                return True
        return False

    def delete_matching_keys(self, bucket_name, prefix, suffix):
        success = True
        matching_keys = self.get_matching_keys(bucket_name=bucket_name, prefix=prefix, suffix=suffix)

        # skip the folder (keys ending with /)
        matching_keys = [key for key in matching_keys if not key.endswith("/")]
        if len(matching_keys) < 1:
            self.logger.info("No matching keys found. Nothing to delete.")
            return success
        else:
            try:
                key_list = [{'Key': k} for k in matching_keys]
                keys_to_delete = {'Objects':  key_list}
                self.s3_resource.meta.client.delete_objects(Bucket=bucket_name, Delete=keys_to_delete)
                self.logger.info("Deleted the following {} keys:\n\t{}".format(len(matching_keys),
                                                                               "\n\t".join(matching_keys)))
            except Exception as e:
                success = False
                self.logger.error(e)

            finally:
                return success

    def write_bytes_to_s3(self, byte_data, bucket_name, key):
        data = BytesIO(byte_data)
        data.seek(0)
        try:
            self.s3_client.upload_fileobj(data, bucket_name, key)
            logging.info("Data uploaded to {}/{}/{}".format(self.s3_client.meta.endpoint_url, bucket_name, key))
        except Exception as e:
            self.logger.error(e)

    def upload_dataframe(self, df, bucket_name, filename, s3_prefix='', file_format='csv',
                         compress=True, delimiter='|', lowercase_headers=True):
        """
        Uploads pandas DataFrame to S3 without writing to disk locally.
        By default the gzip compression is applied and header fields are lower-cased.
        The default encoding is utf-8. File format can be json or csv with specified delimiter.
        """
        full_s3_key = "{}/{}".format(s3_prefix.strip("/"), filename.strip("/"))
        full_s3_key = full_s3_key.strip("/")
        try:
            from pandas import DataFrame
        except ImportError:
            raise Exception("Error importing Pandas DataFrame")

        if not isinstance(df, DataFrame):
            self.logger.error("{} is not a valid Pandas DataFrame".format(df))
            return

        file_format = file_format.lower()
        if file_format not in ['csv', 'json']:
            self.logger.error("Invalid file_format(only csv & json supported).")
            self.logger.info("Using default format i.e. pipe delimited csv")
            file_format = 'csv'
            delimiter = '|'

        if lowercase_headers:
            df.columns = [col.lower() for col in df.columns]

        if compress:
            full_s3_key += '.gz'
            gz_buffer = BytesIO()
            if file_format == 'csv':
                with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
                    df.to_csv(TextIOWrapper(gz_file, 'utf8'),index=False, sep=delimiter, header=True)
            elif file_format == 'json':
                with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
                    df.to_json(TextIOWrapper(gz_file, 'utf8'),orient='records', date_format='iso', lines=True)
            else:
                pass
            self.write_bytes_to_s3(byte_data=gz_buffer.getvalue(), bucket_name=bucket_name, key=full_s3_key)

        else:
            buffer = None
            if file_format == 'csv':
                buffer = df.to_csv(header=True, index=False,sep=delimiter).encode('utf-8')
            elif file_format == 'json':
                buffer = df.to_json(orient='records', date_format='iso', lines=True).encode('utf-8')
            else:
                pass
            self.write_bytes_to_s3(byte_data=buffer, bucket_name=bucket_name, key=full_s3_key)

    def delete_object(self, bucket_name, key):
        # TODO CHECK THIS METHOD AGAIN
        try:
            response = self.s3_resource.Object(bucket_name, key).delete()
            return True

        except Exception as e:
            self.logger.error(e)
            return False

    def copy_object(self, source_bucket, source_key, destination_bucket, destination_path, destination_filename=None):
        if not destination_filename:
            destination_key = '{}/{}'.format(destination_path.strip("/"),
                                             source_key.split("/")[-1])
        else:
            destination_key = '{}/{}'.format(destination_path.strip("/"),
                                             destination_filename)

        source_metadata = {'Bucket': source_bucket, 'Key': source_key}
        try:
            self.s3_resource.meta.client.copy(source_metadata, destination_bucket, destination_key)
            self.logger.info("Successfully copied {} to {}".format(source_key, destination_key))
            return True
        except Exception as e:
            self.logger.error(e)
            return False

    def move_object(self, source_bucket, source_key, destination_bucket, destination_path, destination_filename=None):
        copied = self.copy_object(source_bucket, source_key, destination_bucket, destination_path, destination_filename)
        if copied:
            # If copy is successful, delete source key
            deleted = self.delete_object(bucket_name=source_bucket, key=source_key)
            return deleted
        else:
            return False

    def rename_object(self, source_bucket, source_key, new_filename):
        """
        Simulates renaming of an object by copying data to a new object and deleting old object.
        """
        copied = self.copy_object(source_bucket=source_bucket,
                                  source_key=source_key,
                                  destination_bucket=source_bucket,
                                  destination_path="/".join(source_key.strip("/").split("/")[:-1]),
                                  destination_filename=new_filename)
        if copied:
            deleted = self.delete_object(bucket_name=source_bucket, key=source_key)
            return deleted

    def upload_file(self, filename, bucket_name, key):
        # TODO: allow compression of file before uploading
        try:
            self.s3_resource.meta.client.upload_file(filename, bucket_name, key)
            return True
        except Exception as e:
            self.logger.error(e)
            return False

    def downlaod_file(self):
        pass

    def read_csv(self, bucket_name, key, **kwargs):
        """
        Wrapper around Pandas' read_csv function.This method will not work if the size of
        data is greater than the available memory.

        :bucket_name: name of bucket(str)
        :param key: full key of the file to read
        :param kwargs: keyword arguments supported by pandas read_csv. To get the full list of arguments
                       see official Pandas documentation.
        :return: Pandas DataFrame
        """
        try:
            obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            return pd.read_csv(obj['Body'], **kwargs)
        except Exception as e:
            self.logger.error(e)
            return

    def read_json(self, bucket_name, key, **kwargs):
        """
        Wrapper around the Pandas' read_json.
        :bucket_name: name of bucket(str)
        :param key: full key of the file to read (str)
        :param kwargs: keyword arguments supported by pandas read_json. To get the full list of arguments
                       see official Pandas documentation.
        :return: pandas DataFrame
        """
        try:
            obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            return pd.read_json(obj['Body'], **kwargs)
        except Exception as e:
            self.logger.error(e)
            return

    def read_excel(self, bucket_name, key, **kwargs):
        """
        Wrapper around the Pandas' read_excel.
        :bucket_name: name of bucket(str)
        :param key: full key of the file to read (str)
        :param kwargs: keyword arguments supported by pandas read_excel. To get the full list of arguments
                       see official Pandas documentation.
        :return: pandas DataFrame
        """
        try:
            obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            return pd.read_excel(obj['Body'], **kwargs)
        except Exception as e:
            self.logger.error(e)
            return
