from pathlib import Path

from bdrc_images_and_ocr_downloader.work_info import get_image_keys_and_s3_prefix
from bdrc_images_and_ocr_downloader.download import get_s3_keys, filter_ocr_s3_keys
import shutil
import os

def delete_work_dir(path):
    # Check if the folder exists
    if os.path.exists(path):
        # Recursively delete the folder and all its contents
        shutil.rmtree(path)
        print(f"The folder '{path}' has been deleted.")
    else:
        print(f"The folder '{path}' does not exist.")



def get_all_downloaded_works():
    downloaded_works = []
    ocr_archive_work_list = Path('ocr_archive.txt').read_text().split('\n')
    download_issue_works  = Path('work_with_download_issue.txt').read_text().split('\n')
    work_dirs = list(Path('./data').iterdir())
    work_dirs.sort()
    for work_dir in work_dirs:
        if work_dir.name in ocr_archive_work_list or work_dir.name not in download_issue_works:
            downloaded_works.append(work_dir.name)
    return downloaded_works

def get_work_meta(work_id):
    work_meta = {}
    images_s3_key, s3_prefix = get_image_keys_and_s3_prefix(work_id)
    s3_keys = get_s3_keys(s3_prefix)
    s3_dict = filter_ocr_s3_keys(s3_keys)
    batch_2022 = s3_dict.get('batch_2022', {})
    if batch_2022:
        for img_grp_id, img_grp_res in batch_2022.items():
            work_meta[img_grp_id] = []
            for res in img_grp_res:
                res_type = res.split('/')[-1]
                work_meta[img_grp_id].append(res_type)
    return work_meta

def is_download_completed_work(work_path, work_meta):
    if work_path.exists():
        for img_grp_id, img_grp_res in work_meta.items():
            html_path = work_path / img_grp_id / 'ocr' / 'html.zip'
            if not html_path.exists():
                return False
    else:
        return False
    return True


def filter_download_completed_works():
    complete_works = []
    downloaded_works = Path('ocr_archive_downloaded.txt').read_text().split('\n')
    batch_01_works = Path('batch_01.txt').read_text().split('\n')
    downloaded_works.sort()
    for downloaded_work in downloaded_works:
        work_id = downloaded_work
        work_meta = get_work_meta(downloaded_work)
        work_path = Path(f'./data/{downloaded_work}')
        if not work_meta:
            delete_work_dir(str(work_path))
            continue
        if work_id not in batch_01_works and is_download_completed_work(work_path, work_meta):
            complete_works.append(downloaded_work)
            # print(f'{downloaded_work} is completed')
        else:
            delete_work_dir(str(work_path))
    return complete_works


def get_yet_to_download():
    downloaded_works = Path('download_completed.txt').read_text().split('\n')
    ocr_archive = Path('ocr_archive.txt').read_text().split('\n')
    yet_to_download = list(set(ocr_archive) - set(downloaded_works))
    yet_to_download.sort()
    return yet_to_download



if __name__ == "__main__":
    # downloaded_works = get_all_downloaded_works()
    # downloaded_works.sort()
    # Path('ocr_archive_downloaded.txt').write_text('\n'.join(downloaded_works))
    # complete_works = filter_download_completed_works()
    # Path('./ocr_archive_completed.txt').write_text('\n'.join(complete_works))
    yet_to_download = get_yet_to_download()
    yet_to_download.sort()
    Path('yet_to_download.txt').write_text('\n'.join(yet_to_download))
            
        
