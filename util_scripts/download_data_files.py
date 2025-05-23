import os
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


def download_bz2_files(url, target_directory, file_name):
    os.makedirs(target_directory, exist_ok=True)
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch URL: {e}")
        sys.exit(1)
    soup = BeautifulSoup(response.text, "html.parser")
    links = soup.find_all("a", href=True)
    bz2_links = [
        urljoin(url, link["href"]) for link in links if link["href"].endswith(filename)
    ]
    if not bz2_links:
        print("File not found on the given URL.")
        return
    for file_url in bz2_links:
        if "download=" in file_url:
            file_name = os.path.basename(file_url).split("download=")[0]
        else:
            file_name = os.path.basename(file_url).split("=")[-1]
        target_path = os.path.join(target_directory, file_name)
        try:
            print(f"Downloading {file_url}...")
            file_response = requests.get(file_url, stream=True)
            file_response.raise_for_status()
            with open(target_path, "wb") as file:
                for chunk in file_response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"Saved: {target_path}")
        except requests.RequestException as e:
            print(f"Failed to download {file_url}: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python download_bz2.py <url> <file_name> <target_directory>")
        sys.exit(1)
    input_url = sys.argv[1]
    target_dir = sys.argv[3]
    filename = sys.argv[2]
    download_bz2_files(input_url, target_dir, filename)
