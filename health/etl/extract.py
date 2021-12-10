import requests
import pandas as pd
from zipfile import ZipFile
import os
from io import BytesIO

# TODO:Realizar a etapa de extração dos dados
# 1. Desenvolver componentes para realizar as extrações
# 2. Usar princípio de boas práticas


class ExtractData:
    def extract_zipfile(self, url_zipfile: str, dir: str) -> None:

        filebytes = BytesIO(requests.get(url=url_zipfile, stream=True).content)
        os.makedirs(dir, exist_ok=True)
        files = ZipFile(filebytes)
        files.extractall(path=dir)

    def extract_csvfile(self, read_csv_file: str, path_name_file: str) -> None:

        os.makedirs(path_name_file, exist_ok=True)
        input_file = pd.read_csv(read_csv_file, sep=";", skip_blank_lines=True)
        input_file.to_csv(
            f"{path_name_file}",
            sep=";",
            index=False,
            index_label=False,
            encoding="UTF-8",
        )
