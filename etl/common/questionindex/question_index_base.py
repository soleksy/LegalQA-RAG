import json
import logging
from abc import ABC, abstractmethod

class QuestionIndexBase(ABC):
    def __init__(self):
        with open('etl/common/config.json') as f:
            self.config = json.load(f)

        self.raw_questions_index_path = self.config['raw_questions_index']
        self.raw_questions_data_path = self.config['raw_questions_data']

        self.transformed_questions_index_path = self.config['transformed_questions_index']
        self.transformed_questions_data_path = self.config['transformed_questions_data']

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
    def _get_filename_index(self, domains: list[dict] = None) -> str:
        '''
        Given a list of domains, return the filename of the index file.
        '''
    
    @abstractmethod
    def _get_filename_data(self, domains: list[dict] = None) -> str:
        '''
        Given a list of domains, return the filename of the data file.
        '''
    
    @abstractmethod
    def _find_missing_nros(self, nro_list: list, domains: list[dict] = None) -> list:
        '''
        Given a list of question_nros, check if the index exists and return only not indexed nros.
        '''
    
    @abstractmethod
    def _update_questions_index(self, nro_list: list, domains: list[dict] = None) -> None:
        '''
        Given a list of question_nros, create or update the index.
        '''
    
    @abstractmethod
    def _update_questions_data(self, questions: list[dict] , domains: list[dict] = None) -> list:
        '''
        Given a list of questions, create or update the questions data. Return a list of added question nros.
        '''
    
    @abstractmethod
    def _validate_index(self, domains: list[dict] = None) -> bool:
        '''
        Given a list of domains, check if the set of question_nros in the index is equal to the set of question_nros in the data.
        '''