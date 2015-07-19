import tarfile
import os
from datetime import datetime
from boto3.session import Session

class TarHelper:

    archive_path = None
    target_path = None
    s3_client = None

    def __init__(self, archive_path, target_path, s3_client=None):
        self.archive_path = archive_path
        self.target_path = target_path
        self.s3_client = s3_client

    def delete(self):
        os.remove(self.archive_path)

    def create(self):
        with tarfile.open(self.archive_path, 'w:gz') as tar:
            tar.add(self.target_path, arcname="")

    def extract(self):
        with tarfile.open(self.archive_path, 'r:gz') as tar:
            tar.extractall(self.target_path)

    def tar_to_s3(self, bucket_name, prefix, profile_name=None, delete=True):
        self.create()
        s3_helper = S3BucketHelper(
            bucket_name,
            prefix,
            client=self.s3_client,
            profile_name=None
        )

        s3_helper.upload(self.archive_path)

        if delete:
            self.delete()


class S3BucketHelper:

    _client = None

    profile_name = None
    bucket_name = None
    prefix = None

    @property
    def client(self):
        if self._client is None:
            self._client = Session(profile_name=self.profile_name).client('s3')
        return self._client

    @client.setter
    def client(self, value):
        self._client = value

    def __init__(self, bucket_name=None, prefix=None, profile_name=None, client=None):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.profile_name = profile_name
        self.client = client

    def upload(self, file_path):
        key = '{0}{1}'.format(self.prefix, os.path.basename(file_path))
        data = open(file_path, 'rb')
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=data,
            ServerSideEncryption='AES256',
        )

    def get_most_recent_key(self):
        keys = self.get_most_recent_keys(1)
        return keys[0] if len(keys) > 0 else None

    def get_most_recent_keys(self, number):
        keys = self.get_keys_by_last_modified()
        return keys[0:min(number, len(keys))] if keys else []

    def get_keys_by_last_modified(self, reverse=True):

        next_marker = ''
        max_keys = 1000
        store = {}
        while True:
            response = self.client.list_objects(
                Bucket=self.bucket_name,
                Prefix=self.prefix,
                MaxKeys=max_keys,
                Marker=next_marker
            )

            if 'Contents' not in response:
                return []

            store.update({o['Key']: o['LastModified'] for o in response['Contents']})

            if response['IsTruncated']:
                next_marker = response['Contents'][-1]['Key']
            else:
                break

        return sorted(store, key=store.get, reverse=reverse)

    def download(self, key, destination):
        os.makedirs(os.path.dirname(os.path.abspath(destination)))
        self.client.download_file(self.bucket_name, key, destination)

    def get_tar_helper(self, archive_path, target_path, most_recent=False, key=None):

        if most_recent:
            key = self.get_most_recent_key()

        self.download(key, archive_path)
        return TarHelper(
            os.path.abspath(archive_path),
            target_path,
            s3_client=self.client
        )

    def prune(self, threshold_seconds):
        keys = self.get_keys_by_last_modified()
        min_time = datetime.utcnow() - datetime.timedelta(seconds=threshold_seconds)
        deletes = [o for o in keys if o['LastModified'] < min_time]
        delete_batches = [deletes[i:i+1000] for i in range(0, len(deletes), 1000)]
        for batch in delete_batches:
            self.client.delete_objects(Bucket=self.bucket_name, Deletes={'Objects': batch})
