import os
import boto3
import pytest
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv


def create_s3_client():
    load_dotenv()

    try:
        return boto3.client(
            's3',
            endpoint_url=os.getenv('HOST'),
            aws_access_key_id=os.getenv('ACCESS_KEY'),
            aws_secret_access_key=os.getenv('SECRET_KEY')
            )
    except NoCredentialsError:
        raise Exception(
            'Не удалось подключиться к S3. Проверьте учетные данные.')


def setup_bucket():
    s3 = create_s3_client()

    bucket_name = 'test-bucket'
    s3.create_bucket(Bucket=bucket_name)

    object_name = 'sample.txt'
    s3.put_object(Bucket=bucket_name, Key=object_name)

    return bucket_name, object_name


def teardown_bucket(bucket_name, object_name):
    s3 = create_s3_client()
    s3.delete_object(Bucket=bucket_name, Key=object_name)
    s3.delete_bucket(Bucket=bucket_name)


@pytest.fixture()
def bucket_setup():
    bucket_name, object_name = setup_bucket()
    yield bucket_name, object_name
    teardown_bucket(bucket_name, object_name)


def test_list_objects(bucket_setup):
    s3 = create_s3_client()
    bucket_name, object_name = bucket_setup

    response = s3.list_objects(Bucket=bucket_name)

    assert 'Contents' in response
    assert any(obj['Key'] == object_name for obj in response['Contents'])
