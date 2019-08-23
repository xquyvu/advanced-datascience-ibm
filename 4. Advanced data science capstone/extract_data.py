import zipfile

with zipfile.ZipFile('./data/raw/LANL-Earthquake-Prediction.zip', 'r') as zip_ref:
    zip_ref.extractall('./data/extracted')
