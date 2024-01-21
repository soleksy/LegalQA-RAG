import json
import logging

from abc import ABC

logging.basicConfig(level=logging.WARNING)


class KeywordIndexBase(ABC):

    def __init__(self):
        with open('etl/common/config.json') as f:
            self.config = json.load(f)

        self.raw_keyword_index_path = self.config['raw_keyword_index']
        self.raw_keyword_data_path = self.config['raw_keyword_data']


    def _read_json_file(self, filepath: str):
        try:
            with open(filepath, 'r') as file:
                return json.load(file)
        except FileNotFoundError as e :
            logging.warning(f"File {filepath} not found.")
            raise e
        except json.JSONDecodeError as e:
            logging.warning(f"Error decoding json file {filepath}.")
            raise e
    
    def _write_json_file(self, filepath: str, data: dict | list):
        try:
            with open(filepath, 'w') as file:
                json.dump(data, file , indent=4 , ensure_ascii=False)
        except FileNotFoundError as e:
            logging.warning(f"File {filepath} not found.")
            raise e
        except IOError as e:
            logging.warning(f"Error writing to file {filepath}.")
            raise e
        
    