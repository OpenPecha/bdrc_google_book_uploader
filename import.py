import csv
import os
from pathlib import Path
from tqdm import tqdm
import logging
from openpecha.core.pecha import OpenPechaGitRepo, OpenPechaFS
from concurrent.futures import ProcessPoolExecutor, as_completed
from openpecha.formatters.ocr import OCRFormatter
from openpecha.formatters.ocr.hocr import HOCRFormatter, BDRCGBFileProvider
from openpecha.core.ids import get_initial_pecha_id
from openpecha.buda.api import get_buda_scan_info
import boto3
import os
from pathlib import Path
import rdflib
import requests
import json
import shutil

import hashlib

def get_s3_prefix(work_id, batch_num):
    md5 = hashlib.md5(str.encode(work_id))
    two = md5.hexdigest()[:2]
    folder_prefix = f'Works/{two}/{work_id}/google_books/{batch_num}/'
    return folder_prefix

S3_BUCKET = "ocr.bdrc.io"

def get_ocr_import_info(wlname, batch_num, ocr_info):
    return {
        'source' : 'bdrc',
        'software': 'google_books',
        'batch': batch_num,
        'expected_default_language': 'bo',
        'bdrc_scan_id': wlname,
        'ocr_info': ocr_info
    }

def download_ocr(s3_prefix, ocr_disk_path):
    """
    download all files from s3_prefix into ocr_disk_path
    strip the s3_prefix part of the path when downloading the files
    filter out keys ending in "images.zip"
    """
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # List all objects with the given prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=s3_prefix)
    
    # Download each file
    for page in pages:
        if 'Contents' not in page:
            continue
        for obj in page['Contents']:
            key = obj['Key']
            # Skip files ending with "images.zip"
            if key.endswith("images.zip"):
                continue
            # Get the relative path by removing the prefix
            relative_path = key[len(s3_prefix):].lstrip('/')
            # Create the target file path
            target_path = os.path.join(str(ocr_disk_path), relative_path)
            # Create parent directories if they don't exist
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            # Download the file
            print(f"Downloading {key} to {target_path}")
            s3_client.download_file(S3_BUCKET, key, target_path)
    
    print(f"OCR download complete: {s3_prefix} → {ocr_disk_path}")

def import_w(wlname, batch_num):
    # get buda info for wlname
    buda_info = get_buda_scan_info(wlname)
    tmp_dir = Path("/tmp/gb_op_"+wlname)
    tmp_dir.mkdir(exist_ok=True)
    ocr_disk_path = tmp_dir / "gb"
    ocr_disk_path.mkdir(exist_ok=True)
    s3_prefix = get_s3_prefix(wlname, batch_num)
    #download_ocr(s3_prefix, ocr_disk_path)
    ocr_info = None
    with open(str(ocr_disk_path / 'info.json')) as f:
        ocr_info = json.load(f)
    ocr_import_info = get_ocr_import_info(wlname, batch_num, ocr_info)
    opf_disk_path = tmp_dir / "opf"
    opf_disk_path.mkdir(exist_ok=True)
    file_provider = BDRCGBFileProvider(wlname, buda_info, ocr_import_info, ocr_disk_path=ocr_disk_path)
    formatter = HOCRFormatter(output_path=opf_disk_path)
    pecha_id = get_initial_pecha_id()
    print(f"{wlname},{pecha_id}")
    opf = formatter.create_opf(file_provider, pecha_id, ocr_import_info=ocr_import_info)
    #opf_repo = OpenPechaGitRepo(pecha_id, opf.opf_path)
    #opf_repo.publish()
    shutil.rmtree(str(tmp_dir))

if __name__ == "__main__":
    import_w("W00EGS1016620", "batch_2025")