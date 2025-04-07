from pathlib import Path
import requests
import json
import os
import yaml
import logging

logging.basicConfig(filename='batch_3_opf_catalog.log', level=logging.INFO, format='%(levelname)s - %(message)s')

def read_meta_from_github(pecha_url, meta_path, token):
    # Construct the raw file URL
    raw_url = pecha_url.replace("github.com", "raw.githubusercontent.com") + "/master/" + meta_path
    
    try:
        # Send a GET request to fetch the raw content of the file
        response = requests.get(raw_url, headers={"Authorization": "token " + token})
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Return the content of the file
        return response.text
    except requests.exceptions.RequestException as e:
        print("Error fetching file:", e)
        return None
    

def get_catalog():
    pecha_infos = Path('batch_03_01.txt').read_text().split('\n')
    catalog = ''
    token = os.getenv('GITHUB_TOKEN')
    for pecha_info in pecha_infos:
        work_id, pecha_id = pecha_info.split(',')
        pecha_url = f'https://github.com/OpenPecha-Data/{pecha_id}'
        meta_path = f'{pecha_id}.opf/meta.yml'
        meta = read_meta_from_github(pecha_url, meta_path, token)
        meta_yml = yaml.safe_load(meta)
        source_meta = meta_yml.get('source_metadata', None)
        if source_meta:
            title = source_meta.get('title', '')
        else:
            title = ''
        logging.info(f'{pecha_id},{title},{work_id}')
        catalog += f'{pecha_id},{title},{work_id}\n'
    return catalog


if __name__ == "__main__":
    catalog = get_catalog()
    Path('batch_03_opf_catalog.txt').write_text(catalog)