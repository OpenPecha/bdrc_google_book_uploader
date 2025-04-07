from pathlib import Path

from openpecha.core.ids import get_initial_pecha_id
from openpecha.buda.api import get_buda_scan_info

from openpecha.core.pecha import OpenPechaGitRepo
from openpecha.formatters.ocr import OCRFormatter
from openpecha.formatters.ocr.hocr import HOCRFormatter, BDRCGBFileProvider
from datetime import datetime
import shutil
# from filter_valid_ocr import delete_work_dir
import logging
from utils import get_ocr_import_timestamp

logging.basicConfig(filename='opf_catalog.log', level=logging.INFO, format='%(levelname)s - %(message)s')


def get_current_time_iso8601():
    # Get the current date and time
    now = datetime.now()
    # Convert to ISO 8601 format
    iso8601_format = now.isoformat()
    return iso8601_format

def get_ocr_import_info(work_id):
    import_timestamp = get_ocr_import_timestamp(work_id)
    if not import_timestamp:
        import_timestamp = get_current_time_iso8601()

    ocr_import_info = {
        'source' : 'bdrc',
        'software': 'google_books',
        'batch': 'batch_2022',
        'expected_default_language': 'bo',
        'bdrc_scan_id': work_id,
        'ocr_info': {
            'timestamp': import_timestamp,
            'html': 'html.zip',
            'txt': 'txt.zip',
            'images': 'images.zip'
        }
        
    }

    return ocr_import_info

def prepare_asset(work_dir):
    asset_path = work_dir / 'asset'
    if not asset_path.exists():
        asset_path.mkdir(exist_ok=True)
    for img_grp in work_dir.iterdir():
        if img_grp.stem == 'asset' or ".zip" in img_grp.name:
            continue
        (asset_path / img_grp.stem).mkdir(exist_ok=True)
        html_path = img_grp / 'ocr' / 'html.zip'
        shutil.copy(str(html_path), str(asset_path / img_grp.stem / 'html.zip'))
    
    return asset_path

def get_opf(work_dir):
    bdrc_scan_id = work_dir.stem
    buda_data = get_buda_scan_info(bdrc_scan_id)
    ocr_import_info = get_ocr_import_info(bdrc_scan_id)
    file_provider = BDRCGBFileProvider(bdrc_scan_id, buda_data, ocr_import_info, ocr_disk_path=work_dir)
    formatter = HOCRFormatter(output_path='./opfs')
    pecha_id = get_initial_pecha_id()
    opf = formatter.create_opf(file_provider, pecha_id, opf_options={}, ocr_import_info=ocr_import_info)
    opf_repo = OpenPechaGitRepo(pecha_id, opf.opf_path)
    asset_path = prepare_asset(work_dir)
    asset_name = 'v0.1'
    opf_repo.publish(asset_path, asset_name)
    opf_title = opf.meta.source_metadata.get('title', '')
    return opf.pecha_id, opf_title

if __name__ == "__main__":
    opf_catalog = []
    # complete_ocr_works = Path('ocr_archive_completed.txt').read_text().split('\n')
    complete_ocr_works = Path('./batch_04.txt').read_text().split('\n')
    # complete_ocr_works = ['W8LS66737']
    complete_ocr_works.sort()
    for work_id in complete_ocr_works:
        work_dir = Path(f'data/{work_id}')
        if not work_dir.exists():
            continue
        try:
            opf_id, opf_title = get_opf(work_dir)
            opf_catalog.append(f'{opf_id},{opf_title},{work_id}')
            logging.info(f"OPF created for {work_id} with id {opf_id}")
        except Exception as e:
            logging.info(f"Error creating OPF for {work_id} due to {e}")
            continue
    Path('opf_catalog.txt').write_text('\n'.join(opf_catalog))