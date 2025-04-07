from bdrc_images_and_ocr_downloader.download import download_ocr
from bdrc_images_and_ocr_downloader.work_info import get_image_keys_and_s3_prefix
from pathlib import Path

import logging

logging.basicConfig(filename='downloader.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


if __name__ == '__main__':
    work_ids = Path('yet_to_download.txt').read_text().split('\n')
    work_ids = ['W1KG5966']
    work_ids.sort()
    # complete_ocr_works = Path('batch_01.txt').read_text().split('\n')
    # work_ids = ['W1PD105816']
    for work_id in work_ids:
        work_dir = Path(f'./data/{work_id}')
        # if work_dir.exists():
        #     continue
        # try:
        images_s3_key, s3_prefix = get_image_keys_and_s3_prefix(work_id)
        download_ocr(work_id, s3_prefix)
        # except Exception as e:
        #     logging.error(f"Error downloading {work_id}--{e}")