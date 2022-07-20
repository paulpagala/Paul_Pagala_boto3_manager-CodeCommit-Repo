import logging
import random
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path, PosixPath

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s',
)
log = logging.getLogger()



#for creating a bucket
def create_bucket(name, region=None):
    region = region or 'us-east-2'
    client = boto3.client('s3', region_name=region)
    params = {
        'Bucket': name,
        'CreateBucketConfiguration': {
            'LocationConstraint': region,
        }
    }

    try:
        client.create_bucket(**params)
        return True
    except ClientError as err:
        log.error(f'{err} - Params {params}')
        return False

#============================================================

#for listing buckets
def list_buckets():
    s3 = boto3.resource('s3')

    count = 0
    for bucket in s3.buckets.all():
        print(bucket.name)
        count +=1

    print(f'Found {count} buckets!')

#============================================================

#for getting a bucket
def get_bucket(name, create=False, region=None):
    client = boto3.resource('s3')
    bucket = client.Bucket(name=name)
    if bucket.creation_date:
        return bucket
    else:
        if create:
            create_bucket(name, region=region)
            return get_bucket(name)
        else:
            log.warning(f'Bucket {name} does not exist!')
            return

#===============================================================
#For creating bucket object

def create_tempfile(file_name=None, content=None, size=300):
    """Create a temporary text file"""
    filename = f'{file_name or uuid.uuid4().hex}.txt'
    with open(filename, 'w') as f:
        f.write(f'{(content or "0") * size}')
    return filename

def create_bucket_object(bucket_name, file_path, key_prefix=None):
    """Create a bucket object
    :params bucket_name: The target bucket
    :params type: str
    :params file_path: The path to the file to be uploaded to the bucket
    .
    :params type: str
    :params key_prefix: Optional prefix to set in the bucket for the fil
    e.
    :params type: str
    """
    bucket = get_bucket(bucket_name)
    dest = f'{key_prefix or ""}{file_path}'
    bucket_object = bucket.Object(dest)
    bucket_object.upload_file(Filename=file_path)
    return bucket_object

#==========================================================================
#For getting object

def get_bucket_object(bucket_name, object_key, dest=None, version_id=None):
    """Download a bucket object
    :params bucket_name: The target bucket
    :params type: str
    :params object_key: The bucket object to get
    :params type: str
    :params dest: Optional location where the downloaded file will
    stored in your local.
    :params type: str
    :returns: The bucket object and downloaded file path object.
    :rtype: tuple
    """
    bucket = get_bucket(bucket_name)
    params = {'key': object_key}
    if version_id:
        params['VersionId'] = version_id
    bucket_object = bucket.Object(**params)
    dest = Path(f'{dest or ""}')
    file_path = dest.joinpath(PosixPath(object_key).name)
    bucket_object.download_file(f'{file_path}')
    return bucket_object, file_path
#====================================================
#for enabling bucket version
def enable_bucket_versioning(bucket_name):
    """Enable bucket versioning for the given bucket_name
    """
    bucket = get_bucket(bucket_name)
    versioned = bucket.Versioning()
    versioned.enable()
    return versioned.status
#=============================================================
#for deleting bucket object
def delete_bucket_objects(bucket_name, key_prefix=None):
    """Delete all bucket objects including all versions
    of versioned objects.
    """
    bucket = get_bucket(bucket_name)
    objects = bucket.object_versions
    if key_prefix:
        objects = objects.filter(Prefix=key_prefix)
    else:
        objects = objects.iterator()

    targets = [] # This should be a max of 1000
    for obj in objects:
        targets.append({
            'Key': obj.object_key,
            'VersionId': obj.version_id,
        })
    bucket.delete_objects(Delete={
        'Objects': targets,
        'Quiet': True,
    })
    return len(targets)

#==================================================
#delete buckets
def delete_buckets(name=None):
    count = 0
    if name:
        bucket = get_bucket(name)
        if bucket:
            bucket.delete()
            bucket.wait_until_not_exists()
            count += 1
    else:
        count = 0
        client = boto3.resource('s3')
        for bucket in client.buckets.iterator():
            try:
                bucket.delete()
                bucket.wait_until_not_exists()
                count += 1
            except ClientError as err:
                log.warning(f'Bucket {bucket.name}: {err}')

    return count

if __name__=='__main__':
    #create bucket
    
    bucket_create = create_bucket('testawesomeprojectpj')
    log.info(f'{bucket_create}')
    
    
    bucket_list=list_buckets()
    log.info(f'{bucket_list}')
    
    
    bucket_get = get_bucket('testawesomeprojectpj').creation_date
    log.info(f'{bucket_get}')
    
    
    tmp_file = create_tempfile()
    log.info(f'{tmp_file}')

    b_obj = create_bucket_object('testawesomeprojectpj', tmp_file, key_prefix='temp/')
    log.info(f'{b_obj}')

    tmp_file = Path(tmp_file)
    log.info(f'{tmp_file.exists()}')

    unlink_file = tmp_file.unlink()
    log.info(f'{unlink_file}')

    exists_file = tmp_file.exists()
    log.info(f'{exists_file}')

    bucket_obj_key = b_obj.key
    log.info(f'{bucket_obj_key}')

    b_obj, tmp_file = get_bucket_object('testawesomeprojectpj', bucket_obj_key)
    log.info(f'{b_obj.key}')
    log.info(f'{tmp_file}')

    enable_versioning = enable_bucket_versioning('testawesomeprojectpj')
    log.info(f'{enable_versioning}')

    open_read = tmp_file.open().read()
    log.info(f'{open_read}')

    write_tmp_file = tmp_file.open(mode='w').write('10' * 500)
    log.info(f'{write_tmp_file}')

    new_open_read = tmp_file.open().read()
    log.info(f'{new_open_read}')

    new_create_bucket_object = create_bucket_object('testawesomeprojectpj', tmp_file.name, key_prefix='temp/')
    log.info(f'{new_create_bucket_object}')

    list_buckets = list(get_bucket('testawesomeprojectpj').objects.all())
    log.info(f'{list_buckets}')

    list_buckets_versions = list(get_bucket('testawesomeprojectpj').object_versions.all())
    log.info(f'{list_buckets_versions}')
    
    
    for _ in range(3):
        obj = create_bucket_object(
            'testawesomeprojectpj',
            file_path=create_tempfile(),
            key_prefix='others/'
        )
        print(f'Object {obj.key} created!')

    latest_version_all = list(get_bucket('testawesomeprojectpj').objects.all())
    log.info(f'{latest_version_all}')

    temp_version_all = list(get_bucket('testawesomeprojectpj').objects.filter(Prefix='temp/'))
    log.info(f'{temp_version_all}')

    delete_object = get_bucket('testawesomeprojectpj').objectsfilter(Prefix='temp/').delete()
    log.info(f'{delete_object}')

    deleted_objects = delete_objects('testawesomeprojectpj', key_prefix = 'temp/')
    log.info(f'{deleted_objects}')

    deleted_object = delete_bucket_objects('testawesomeprojectpj')
    log.info(f'{deleted_object}')

    bucket_delete = delete_buckets('testawesomeprojectpj')
    log.info(f'{bucket_delete}')
    
