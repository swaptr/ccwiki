import os
import gzip
import re
import requests
import subprocess

import pandas as pd
import xml.etree.ElementTree as ET

from tqdm import tqdm

location = "./files"

def download_one(id):
    chunk_size = 1024
    info = get_info_for_id(id)
    url = info["item_href"]
    file_name = info["file_name"]
    file_location = os.path.join(location, file_name)
    ext_location = os.path.join(location, f"{id}.sql")

    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))

    with open(file_location, "wb") as handle:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading {id}") as progress_bar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                handle.write(chunk)
                progress_bar.update(len(chunk))

    total_size = os.path.getsize(file_location)

    with gzip.open(file_location, 'rb') as f_in:
        with open(ext_location, 'wb') as f_out:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=f'Extracting {id}') as pbar:
                chunk_size = 1024 * 1024
                while True:
                    chunk = f_in.read(chunk_size)
                    if not chunk:
                        break
                    f_out.write(chunk)
                    pbar.update(len(chunk))

def get_list_of_languages():
    df = pd.read_html("https://meta.wikimedia.org/wiki/List_of_Wikipedias")[0]
    return list(df["Wiki"].str.replace("-", "_"))

def get_info_for_id(id: str):
    try:
        response = requests.get(
            f'https://dumps.wikimedia.org/{id}wiki/latest/{id}wiki-latest-externallinks.sql.gz-rss.xml',
        )
        response.raise_for_status()

        root = ET.fromstring(response.text)
        
        channel = root.find('channel')
        if channel is None:
            return {"error": "Channel not found in the RSS feed"}

        item = channel.find('item')
        if item is None:
            return {"error": "No items found in the RSS feed"}

        item_description = item.findtext('description', default='')
        match = re.search(r'<a href="(.*?)">(.*?)</a>', item_description)

        return {
            'channel_title': channel.findtext('title', default=''),
            'channel_link': channel.findtext('link', default=''),
            'channel_description': channel.findtext('description', default=''),
            'item_title': item.findtext('title', default=''),
            'item_link': item.findtext('link', default=''),
            'item_pub_date': item.findtext('pubDate', default=''),
            'item_href': match.group(1) if match else None,
            'file_name': match.group(2) if match else None
        }

    except requests.RequestException as e:
        return {"error": f"HTTP request failed: {str(e)}"}
    except ET.ParseError:
        return {"error": "Failed to parse XML response"}
    except Exception as e:
        return {"error": f"An unexpected error occurred: {str(e)}"}

def download_all(languages = get_list_of_languages()):
    for index, language in enumerate(languages):
        print(f"{index + 1}/{len(languages)}")
        download_one(language)

def convert_to_sqlite(id):
    mysql2sqlite_script = './mysql2sqlite/mysql2sqlite'
    sql_file_path = f"./files/{id}.sql"
    sqlite_db_path = f"./files/{id}.db"
    
    command = [mysql2sqlite_script, sql_file_path]

    try:
        print(f"Converting hi: {sql_file_path}")
        subprocess.run(command, stdout=subprocess.PIPE, check=True)
        
        sqlite_command = ['sqlite3', sqlite_db_path]
        
        with subprocess.Popen(command, stdout=subprocess.PIPE) as mysql_process:
            with subprocess.Popen(sqlite_command, stdin=mysql_process.stdout) as sqlite_process:
                sqlite_process.wait()
        
        print(f"Converted hi: {sqlite_db_path}")
    
    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    os.makedirs(location, exist_ok=True)

    # download_all()
    # download_one("hi")
    convert_to_sqlite("hi")