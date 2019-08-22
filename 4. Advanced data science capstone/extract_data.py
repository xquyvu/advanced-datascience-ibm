import zipfile

with zipfile.ZipFile('./data/raw/dutch-energy.zip', 'r') as zip_ref:
    zip_ref.extractall('./data/extracted')
