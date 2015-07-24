import tarfile
import os
import dateutil.parser
from datetime import datetime
import time
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from jinja2 import Template


class NoRecentKeyFound(Exception):
    pass


class TarHelper:

    archive_path = None
    target_path = None

    def __init__(self, archive_path, target_path):
        self.archive_path = archive_path
        self.target_path = target_path

    def delete(self):
        os.remove(self.archive_path)

    def create(self):
        with tarfile.open(self.archive_path, 'w:gz') as tar:
            tar.add(self.target_path, arcname="")

    def extract(self):
        with tarfile.open(self.archive_path, 'r:gz') as tar:
            tar.extractall(self.target_path)

    def tar_to_s3(self, bucket_name, prefix, delete=True):
        self.create()
        s3_helper = S3BucketHelper(
            bucket_name,
            prefix
        )

        s3_helper.upload(self.archive_path)

        if delete:
            self.delete()

    @classmethod
    def tar_to_s3_entry(cls, opts):
        archive = Template(opts.key_format).render(
            directory=os.path.basename(opts.directory.rstrip('/')),
            timestamp=(int(time.time()))
        )

        tar = cls(archive, opts.directory)
        tar.tar_to_s3(opts.bucket, opts.prefix)

    @classmethod
    def untar_from_s3_entry(cls, opts):

        bh = S3BucketHelper(opts.bucket, opts.prefix)

        tar = bh.get_tar_helper(
            opts.directory,
            opts.save_as,
            cwd=opts.cwd,
            most_recent=opts.key is None,
            key=opts.key
        )

        if tar is None and opts.key is None:
            if opts.ignore_missing:
                return False
            else:
                raise NoRecentKeyFound

        tar.extract()
        if opts.delete:
            tar.delete()

        return True


class S3BucketHelper:

    _client = None
    _bucket = None

    profile_name = None
    bucket_name = None
    prefix = None

    @property
    def bucket(self):
        if self._bucket is None:
            self._bucket = self.client.get_bucket(
                self.bucket_name,
                validate=False
            )
        return self._bucket

    @property
    def client(self):
        if self._client is None:
            self._client = S3Connection()
        return self._client

    @client.setter
    def client(self, value):
        self._client = value

    def __init__(self, bucket_name=None, prefix=None):
        self.bucket_name = bucket_name
        self.prefix = prefix

    def upload(self, file_path):
        keyname = '{0}{1}'.format(self.prefix, os.path.basename(file_path))
        key = Key(self.bucket, keyname)
        key.set_contents_from_filename(file_path, encrypt_key=True)

    def get_most_recent_key(self):
        keys = self.get_most_recent_keys(1)
        return keys[0] if len(keys) > 0 else None

    def get_most_recent_keys(self, number):
        keys = [x['name'] for x in self.get_keys_by_last_modified()]
        return keys[0:min(number, len(keys))] if keys else []

    def get_keys_by_last_modified(self, reverse=True):

        store = [
            {'name': o.name,
             'time': time.mktime(dateutil.parser.parse(o.last_modified).utctimetuple())}
            for o in self.bucket.list(prefix=self.prefix)]

        store.sort(key=lambda j: j['time'], reverse=reverse)
        return store

    def download(self, key, destination):

        dir_path = os.path.dirname(os.path.abspath(destination))
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)

        Key(self.bucket, key).get_contents_to_filename(destination)

    def get_tar_helper(self, target_path, archive_path=None, most_recent=False, key=None, cwd=None):

        if most_recent:
            key = self.get_most_recent_key()
            if key is None:
                return None

        if archive_path is None:
            archive_path = os.path.basename(key)

        if cwd is not None:
            archive_path = os.path.join(cwd, archive_path)

        self.download(key, archive_path)
        return TarHelper(
            os.path.abspath(archive_path),
            target_path
        )

    def prune(self, threshold_seconds):
        keys = self.get_keys_by_last_modified()
        min_time = time.mktime(datetime.utcnow().utctimetuple()) - float(threshold_seconds)

        deletes = [o['name'] for o in keys if o['time'] < min_time]

        self.bucket.delete_keys(deletes)

    @classmethod
    def prune_entry(cls, opts):

        threshold = None

        if opts.days is not None:
            threshold = float(opts.days*86400)
        elif opts.seconds is not None:
            threshold = float(opts.seconds)

        if threshold is not None:
            cls(opts.bucket, opts.prefix).prune(threshold)
