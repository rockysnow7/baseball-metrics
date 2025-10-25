import os
import urllib.request
import zipfile


URL_TEMPLATE = "https://www.retrosheet.org/downloads/{year}/{year}csvs.zip"

def download_retrosheet_data(year: int):
    url = URL_TEMPLATE.format(year=year)
    os.makedirs(f"retrosheet/{year}", exist_ok=True)
    zip_path = f"retrosheet/{year}/{year}csvs.zip"

    print(f"Downloading retrosheet data for {year}...")
    urllib.request.urlretrieve(url, zip_path)

    print(f"Unzipping {zip_path}...")
    with zipfile.ZipFile(zip_path) as zip_file:
        zip_file.extractall(f"retrosheet/{year}")
    print("Done!")
    os.remove(zip_path)
