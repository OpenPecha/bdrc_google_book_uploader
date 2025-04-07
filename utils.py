from bdrc_images_and_ocr_downloader.download import get_s3_keys
from bdrc_images_and_ocr_downloader.work_info import get_image_keys_and_s3_prefix
from bdrc_images_and_ocr_downloader.config import s3_client, BDRC_ARCHIVE_BUCKET, OCR_OUTPUT_BUCKET
import gzip
import json
import botocore
import gzip
import io
import os
from pathlib import Path


def filter_batch_s3_keys(s3_keys, batch_name):
    batch_keys = []
    for key in s3_keys:
        key_parts = key.split('/')
        if len(key_parts) < 5:
            continue
        batch = key_parts[4]
        if batch == batch_name:
            batch_keys.append(key)

    return batch_keys

def get_img_grp_gb_bdrc_mapping(work_id):
    mages_s3_key, s3_prefix = get_image_keys_and_s3_prefix(work_id)
    s3_keys = get_s3_keys(s3_prefix)
    batch_2022_keys = filter_batch_s3_keys(s3_keys, batch_name='batch_2022')
    img_gb_bdrc_mapping = {}
    for key in batch_2022_keys:
        file_name = key.split('/')[-1]
        img_grp = key.split('/')[-2]
        meta = key.split('/')[-3]
        if file_name.endswith('.json') and meta == 'info':
            img_gb_bdrc_mapping[img_grp] = key
    return img_gb_bdrc_mapping

def gets3blob(s3Key):
    f = io.BytesIO()
    try:
        s3_client.download_fileobj("ocr.bdrc.io", s3Key, f)
        return f
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return None
        else:
            raise

def get_img_gb_mapping(work_id, img_grp_id, img_gb_bdrc_mapping):
    img_grp_path = f'{work_id}-{img_grp_id}'
    img_grp_mapping_path = img_gb_bdrc_mapping.get(img_grp_path)
    if img_grp_mapping_path is None and 'I' == img_grp_id[0]:
        img_grp_path = f'{work_id}-{img_grp_id[1:]}'
        img_grp_mapping_path = img_gb_bdrc_mapping.get(img_grp_path)
    if img_grp_mapping_path is None:
        return None
    blob = gets3blob(img_grp_mapping_path)
    if blob is None:
        return None
    blob.seek(0)
    b = blob.read()
    img_grp_mapping = json.loads(b)
    return img_grp_mapping

def get_ocr_import_timestamp(work_id):
    mages_s3_key, s3_prefix = get_image_keys_and_s3_prefix(work_id)
    s3_keys = get_s3_keys(s3_prefix)
    img_info_key = ''
    for key in s3_keys:
        key_parts = key.split('/')
        if key_parts[4] == 'batch_2022':
            if key_parts[5] == 'info.json':
                img_info_key = key
                break
    try:
        os.system(f'aws s3 cp s3://{OCR_OUTPUT_BUCKET}/{img_info_key} ./info.json')
        ocr_import_info = json.loads(open('./info.json').read())
        ocr_import_timestamp = ocr_import_info.get('timestamp', None)
        Path('./info.json').unlink()
    except:
        ocr_import_timestamp = None
    return ocr_import_timestamp
        

if __name__ == "__main__":
    work_id = 'W1PD95844'
    img_mapping = get_img_grp_gb_bdrc_mapping(work_id)
    # img_grp_id = 'I1KG15468'
    # img_grp_mapping = get_img_gb_mapping(work_id, img_grp_id, img_mapping)
    # ocr_info = get_ocr_info(work_id)