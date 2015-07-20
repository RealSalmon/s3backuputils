import uuid
import shutil
import os.path
import time
from nose.tools import *
from moto import mock_s3
from s3backuputils import TarHelper, S3BucketHelper
from boto.s3.connection import S3Connection
from boto.s3.key import Key

_outdir = os.path.join(os.path.dirname(__file__), 'outfiles')


def setup():
    if not os.path.isdir(_outdir):
        os.mkdir(_outdir)


def teardown():
    if os.path.isdir:
        shutil.rmtree(_outdir)


def test_basic():
    os.path.isdir(_outdir)


def test_write():
    with open(_outdir+'/testout', 'w') as f:
        f.write('hey!')

    assert os.path.isfile(_outdir+'/testout')

@mock_s3
def test_write_to_s3():

    filepath = os.path.join(_outdir, str(uuid.uuid4()))
    bucket_name = str(uuid.uuid4())
    contents = str(uuid.uuid4())

    with open(filepath, 'w') as f:
        f.write(contents)

    conn = S3Connection()
    conn.create_bucket(bucket_name)

    bucket = conn.get_bucket(bucket_name)
    key = Key(bucket, os.path.basename(filepath))
    key.set_contents_from_filename(filepath)

    xkey = Key(bucket, os.path.basename(filepath))
    assert xkey.get_contents_as_string() == contents


@mock_s3
def test_create_s3_bucket():
    bucket_name = str(uuid.uuid4())
    conn = S3Connection()
    conn.create_bucket(bucket_name)

    response = conn.get_all_buckets()
    assert bucket_name == response[0].name


@mock_s3
def test_tar_to_s3():

    bucket_name = str(uuid.uuid4())
    prefix = '{0}/'.format(str(uuid.uuid4()))
    tar_name = '{0}.tar.gz'.format(str(uuid.uuid4()))

    conn = S3Connection()
    conn.create_bucket(bucket_name)

    tar = TarHelper(
        os.path.join(_outdir, tar_name),
        os.path.join(os.path.dirname(__file__), 'testfiles')
    )
    tar.tar_to_s3(bucket_name=bucket_name, prefix=prefix)

    bucket = S3BucketHelper(
        bucket_name=bucket_name,
        prefix=prefix
    )

    assert bucket.get_most_recent_key() == '{0}{1}'.format(prefix, tar_name)


@mock_s3
def test_untar_from_s3():

    bucket_name = str(uuid.uuid4())
    prefix = '{0}/'.format(str(uuid.uuid4()))
    tar_name = '{0}.tar.gz'.format(str(uuid.uuid4()))

    conn = S3Connection()
    conn.create_bucket(bucket_name)

    tar = TarHelper(
        os.path.join(_outdir, tar_name),
        os.path.join(os.path.dirname(__file__), 'testfiles')
    )
    tar.tar_to_s3(bucket_name=bucket_name, prefix=prefix)

    bucket = S3BucketHelper(
        bucket_name=bucket_name,
        prefix=prefix
    )

    assert bucket.get_most_recent_key() == '{0}{1}'.format(prefix, tar_name)

    archive_path = os.path.join(
        _outdir,
        str(uuid.uuid4()),
        '{0}.tar.gz'.format(str(uuid.uuid4()))
    )
    target_path = os.path.join(_outdir, str(uuid.uuid4()))

    tar2 = bucket.get_tar_helper(
        most_recent=True,
        archive_path=archive_path,
        target_path=target_path
    )

    assert os.path.isfile(archive_path)

    tar2.extract()

    assert os.path.isdir(target_path)
    for xfile in range(1, 10):
            assert os.path.isfile(os.path.join(target_path, 'file_{0}'.format(xfile)))

    for xdir in range(1, 3):
        dirpath = os.path.join(target_path, 'dir_{0}'.format(xdir))
        assert os.path.isdir(dirpath)
        for xfile in range(1, 10):
            assert os.path.isfile(os.path.join(dirpath, 'file_{0}'.format(xfile)))


@mock_s3
def test_prune():

    bucket_name = str(uuid.uuid4())
    prefix = '{0}/'.format(str(uuid.uuid4()))

    conn = S3Connection()
    conn.create_bucket(bucket_name)

    bucket = conn.get_bucket(bucket_name)

    for i in range(2000):
        key_name = '{0}{1}'.format(prefix, str(uuid.uuid4()))
        key = Key(bucket, key_name)
        key.set_contents_from_string(str(uuid.uuid4()))

    helper = S3BucketHelper(bucket_name, prefix)
    assert len(helper.get_keys_by_last_modified()) == 2000

    time.sleep(15)

    keys = []
    for i in range(100):
        key_name = '{0}{1}'.format(prefix, str(uuid.uuid4()))
        keys.append(key_name)
        key = Key(bucket, key_name)
        key.set_contents_from_string(str(uuid.uuid4()))

    assert len(helper.get_keys_by_last_modified()) == 2100
    helper.prune(10)

    helper2 = S3BucketHelper(bucket_name, prefix)
    current_keys = helper2.get_keys_by_last_modified()
    assert len(current_keys) == 100
    assert len([k for k in current_keys if k['name'] in keys]) == 100
