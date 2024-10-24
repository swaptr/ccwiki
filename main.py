import os
import gzip
import re
import requests
import subprocess

import pandas as pd
import xml.etree.ElementTree as ET

from io import StringIO
from tqdm import tqdm

location = "./files"

def get_iso_languages_df():
    """Fetches the latest ISO-639-3 language codes."""
    response = requests.get("https://iso639-3.sil.org/sites/iso639-3/files/downloads/iso-639-3_Name_Index.tab")

    if response.status_code == 200:
        data = StringIO(response.content.decode('utf-8'))
        return pd.read_csv(data, sep='\t', encoding='utf-8')
    else:
        print(f"Failed to retrieve data: {response.status_code}")
        return None

def generate_iso_code_wiki_csv():
    """Generates a CSV file with ISO-639-3 language codes."""
    iso_df = get_iso_languages_df()
    lang_df = pd.read_html("https://meta.wikimedia.org/wiki/List_of_Wikipedias")[0][["Language", "Wiki"]]
    found = []
    not_found = []
    conflicts = []
    for _, lang in lang_df.iterrows():
        if len(lang.Wiki) == 3:
            iso_match_df = iso_df[iso_df["Id"] == lang.Wiki]
            match len(iso_match_df):
                case 0:
                    not_found.append({
                        "wiki_code": lang.Wiki,
                        "wiki_name": lang.Language,
                        "iso_code": None,
                        "iso_name": None
                    })
                case 1:
                    found.append({
                        "wiki_code": lang.Wiki,
                        "wiki_name": lang.Language,
                        "iso_code": iso_match_df.Id.iloc[0],
                        "iso_name": iso_match_df.Print_Name.iloc[0]
                    })
                case _:
                    lang_match_df = iso_df[iso_df["Print_Name"] == lang.Language]
                    match len(lang_match_df):
                        case 0:
                            conflicts.append({
                                "wiki_code": lang.Wiki,
                                "wiki_name": lang.Language,
                                "conflicts": iso_match_df.Print_Name.tolist()
                            })
                        case 1:
                            found.append({
                                "wiki_code": lang.Wiki,
                                "wiki_name": lang.Language,
                                "iso_code": lang_match_df.Id.iloc[0],
                                "iso_name": lang_match_df.Print_Name.iloc[0]
                            })
                        case _:
                            conflicts.append({
                                "wiki_code": lang.Wiki,
                                "wiki_name": lang.Language,
                                "conflicts": lang_match_df.Print_Name.tolist()
                            })
        else:
            lang_match_df = iso_df[iso_df["Print_Name"] == lang.Language]
            match len(lang_match_df):
                case 0:
                    not_found.append({
                        "wiki_code": lang.Wiki,
                        "wiki_name": lang.Language,
                        "iso_code": None,
                        "iso_name": None
                    })
                case 1:
                    found.append({
                        "wiki_code": lang.Wiki,
                        "wiki_name": lang.Language,
                        "iso_code": lang_match_df.Id.iloc[0],
                        "iso_name": lang_match_df.Print_Name.iloc[0]
                    })
                case _:
                    conflicts.append({
                        "wiki_code": lang.Wiki,
                        "wiki_name": lang.Language,
                        "conflicts": lang_match_df.Print_Name.tolist()
                    })

    pd.DataFrame(found).to_csv(f"./{location}/iso_languages.csv", index=None)

def download_one(id):
    """Download database dump for a single language wiki."""
    chunk_size = 1024
    info = get_info_for_id(id)
    url = info["item_href"]
    file_name = info["file_name"]
    file_location = os.path.join(location, file_name)
    ext_location = os.path.join(location, f"{id}.sql")

    response = requests.get(url, stream=True)
    total_size = int(response.headers.get("content-length", 0))

    with open(file_location, "wb") as handle:
        with tqdm(
            total=total_size, unit="B", unit_scale=True, desc=f"Downloading {id}"
        ) as progress_bar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                handle.write(chunk)
                progress_bar.update(len(chunk))

    total_size = os.path.getsize(file_location)

    with gzip.open(file_location, "rb") as f_in:
        with open(ext_location, "wb") as f_out:
            with tqdm(
                total=total_size, unit="B", unit_scale=True, desc=f"Extracting {id}"
            ) as pbar:
                chunk_size = 1024 * 1024
                while True:
                    chunk = f_in.read(chunk_size)
                    if not chunk:
                        break
                    f_out.write(chunk)
                    pbar.update(len(chunk))


def get_list_of_languages():
    """Returns list of language wikis."""
    df = pd.read_html("https://meta.wikimedia.org/wiki/List_of_Wikipedias")[0]
    return list(df["Wiki"].str.replace("-", "_"))


def get_info_for_id(id: str):
    """Returns information from the RSS feed for a language."""
    try:
        response = requests.get(
            f"https://dumps.wikimedia.org/{id}wiki/latest/{id}wiki-latest-externallinks.sql.gz-rss.xml",
        )
        response.raise_for_status()

        root = ET.fromstring(response.text)

        channel = root.find("channel")
        if channel is None:
            return {"error": "Channel not found in the RSS feed"}

        item = channel.find("item")
        if item is None:
            return {"error": "No items found in the RSS feed"}

        item_description = item.findtext("description", default="")
        match = re.search(r'<a href="(.*?)">(.*?)</a>', item_description)

        return {
            "channel_title": channel.findtext("title", default=""),
            "channel_link": channel.findtext("link", default=""),
            "channel_description": channel.findtext("description", default=""),
            "item_title": item.findtext("title", default=""),
            "item_link": item.findtext("link", default=""),
            "item_pub_date": item.findtext("pubDate", default=""),
            "item_href": match.group(1) if match else None,
            "file_name": match.group(2) if match else None,
        }

    except requests.RequestException as e:
        return {"error": f"HTTP request failed: {str(e)}"}
    except ET.ParseError:
        return {"error": "Failed to parse XML response"}
    except Exception as e:
        return {"error": f"An unexpected error occurred: {str(e)}"}


def download_all(languages=get_list_of_languages()):
    """Download all languages and extract the MySQL database dump.

    Accepts a list of languages to download. By default, it downloads all available languages.
    """

    for index, language in enumerate(languages):
        print(f"{index + 1}/{len(languages)}")
        download_one(language)


def convert_to_sqlite(id):
    """Transforms a MySQL dump into a SQLite database.

    Please ensure that you have properly configured and updated the git submodules.
    """
    mysql2sqlite_script = "./mysql2sqlite/mysql2sqlite"
    sql_file_path = f"./files/{id}.sql"
    sqlite_db_path = f"./files/{id}.db"

    command = [mysql2sqlite_script, sql_file_path]

    try:
        print(f"Converting hi: {sql_file_path}")
        subprocess.run(command, stdout=subprocess.PIPE, check=True)

        sqlite_command = ["sqlite3", sqlite_db_path]

        with subprocess.Popen(command, stdout=subprocess.PIPE) as mysql_process:
            with subprocess.Popen(
                sqlite_command, stdin=mysql_process.stdout
            ) as sqlite_process:
                sqlite_process.wait()

        print(f"Converted hi: {sqlite_db_path}")

    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    os.makedirs(location, exist_ok=True)

    # download_all()
    # download_one("st")
    # convert_to_sqlite("st")
    # generate_iso_code_wiki_csv()