import json
import logging
from abc import ABC , abstractmethod

class ActIndexBase(ABC):
    def __init__(self):
        with open('etl/common/config.json') as f:
            self.config = json.load(f)

        self.tree_acts_index_path = self.config['tree_acts_index']
        self.tree_acts_data_path = self.config['tree_acts_data']

        self.leaf_node_acts_index_path = self.config['leaf_node_acts_index']
        self.leaf_node_acts_data_path = self.config['leaf_node_acts_data']

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
    
    @abstractmethod
    def _get_filename_index(self) -> str:
        '''
        Return the filename of the index file
        '''
    
    @abstractmethod
    def _get_filename_data(self, act_nro: int) -> str:
        '''
        Return the data of the act with the given act_nro
        '''
    
    @abstractmethod
    def _find_missing_nros(self, nro_list: list[int]) -> list[int]:
        '''
        Return the missing act_nros from the given list
        '''

    @abstractmethod
    def _update_act_index(self, nro_list: list[int]) -> None:
        '''
        Update the index file with the given list of act_nros
        '''
    
    @abstractmethod
    def _update_act_data(self, act_nro: int, act_data: dict) -> None:
        '''
        Update the data file with the given act_data
        '''

    @abstractmethod
    def _validate_act_index(self) -> bool:
        '''
        Validate the index file
        '''