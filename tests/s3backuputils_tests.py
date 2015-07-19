import uuid
import shutil
import os.path
from nose.tools import *
from moto import mock_s3
from boto3.session import Session
from s3backuputils import TarHelper, S3BucketHelper

_outdir = os.path.join(os.path.dirname(__file__), 'outfiles')


def setup():
    if not os.path.isdir(_outdir):
        os.mkdir(_outdir)


def teardown():
    if os.path.isdir:
        shutil.rmtree(_outdir)


def test_basic():
    os.path.isdir(_outdir)


@mock_s3
def test_mock_s3_bucket():
    bucket_name = str(uuid.uuid4())
    client = Session().client('s3')
    client.create_bucket(Bucket=bucket_name)
    response = client.list_buckets()
    assert bucket_name == response['Buckets'][0]['Name']


@mock_s3
def test_tar_to_s3():

    bucket_name = str(uuid.uuid4())
    prefix = '{0}/'.format(str(uuid.uuid4()))
    tar_name = '{0}.tar.gz'.format(str(uuid.uuid4()))

    client = Session().client('s3')
    client.create_bucket(Bucket=bucket_name)

    tar = TarHelper(
        os.path.join(_outdir, tar_name),
        os.path.join(os.path.dirname(__file__), 'testfiles'),
        s3_client=client
    )
    tar.tar_to_s3(bucket_name=bucket_name, prefix=prefix)

    bucket = S3BucketHelper(
        bucket_name=bucket_name,
        prefix=prefix,
        client=client
    )

    assert bucket.get_most_recent_key() == '{0}{1}'.format(prefix, tar_name)


@mock_s3
def test_untar_from_s3():

    bucket_name = str(uuid.uuid4())
    prefix = '{0}/'.format(str(uuid.uuid4()))
    tar_name = '{0}.tar.gz'.format(str(uuid.uuid4()))

    client = Session().client('s3')
    client.create_bucket(Bucket=bucket_name)

    tar = TarHelper(
        os.path.join(_outdir, tar_name),
        os.path.join(os.path.dirname(__file__), 'testfiles'),
        s3_client=client
    )
    tar.tar_to_s3(bucket_name=bucket_name, prefix=prefix)

    bucket = S3BucketHelper(
        bucket_name=bucket_name,
        prefix=prefix,
        client=client
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
