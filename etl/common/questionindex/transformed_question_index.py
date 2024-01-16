import os
import logging
import datetime

from etl.common.questionindex.question_index_base import QuestionIndexBase

class TransformedQuestionIndex(QuestionIndexBase):
    def __init__(self) -> None:
        super().__init__()

    def _get_filename_index(self, domains: list[dict] = None) -> str:
        '''
        Given a list of domains, return the filename of the index file.
        '''

        if domains is not None:
            return '_'.join(str(list(d.values())[0]) for d in domains) + '_transformed_index.json'
        else:
            return 'all_transformed_index.json'
        
    def _get_filename_data(self, domains: list[dict] = None) -> str:
        '''
        Given a list of domains, return the filename of the data file.
        '''

        if domains is not None:
            return '_'.join(str(list(d.values())[0]) for d in domains) + '_transformed_data.json'
        else:
            return 'all_transformed_data.json'
        
    def _find_missing_nros(self, nro_list: list[str], domains: list[dict] = None) -> list[str]:
        '''
        Given a list of question_nros, check if the index of transformed questions exists and return only not indexed nros.
        '''

        file_name = self._get_filename_index(domains=domains)
        file_path = self.transformed_questions_index_path+file_name

        if os.path.exists(file_path):

            index = self._read_json_file(file_path)
            index = [str(nro) for nro in index]

            missing = list(set(nro_list).difference(set(index)))

            if len(missing) > 0:
                return missing
            else:
                return []
        else:
            return nro_list
    
    def _update_questions_index(self, nro_list: list[str] , domains: list[dict] = None) -> None:
        '''
        Given a list of question_nros, create or update the index of transformed questions.
        '''

        nro_list = [int(nro) for nro in nro_list]
        
        file_name = self._get_filename_index(domains=domains)
        file_path = self.transformed_questions_index_path+file_name

        if os.path.exists(file_path):
            index = self._read_json_file(file_path)

            nro_list = list(set(nro_list + index))

            self._write_json_file(file_path, nro_list)
        else:
            self._write_json_file(file_path, nro_list)

    def _update_questions_data(self, questions: list[dict] , domains: list[dict] = None) -> list[str]:
        '''
        Given a list of questions, create or update the transformed questions data.
        '''

        file_name = self._get_filename_data(domains=domains)
        result_nros = []
        
        file_path = self.transformed_questions_data_path+file_name

        if os.path.exists(file_path):
            data = self._read_json_file(file_path)

            question_keys = set(data['questions'].keys())

            for question in questions:
                if str(question['nro']) not in question_keys:
                    result_nros.append(question['nro'])
                    data['questions'][question['nro']] = question
                else:
                    logging.warning(f"Question with nro {question['nro']} already exists in data file.")

            self._write_json_file(file_path, data)
        else:
            result_nros = [str(question['nro']) for question in questions]
            self._write_json_file(file_path, {'last_update': str(datetime.datetime.now()).split()[0], 'questions': {question['nro']: question for question in questions}})

        return result_nros
    
    def _validate_index(self, domains: list[dict] = None) -> bool:
        '''
        Given a list of domains, check if the set of question_nros in the index is equal to the set of question_nros in the data.
        '''

        file_name_index = self._get_filename_index(domains=domains)
        file_name_data = self._get_filename_data(domains=domains)

        file_path_index = self.transformed_questions_index_path+file_name_index
        file_path_data = self.transformed_questions_data_path+file_name_data

        if os.path.exists(file_path_index) and os.path.exists(file_path_data):

            index_question_nros = self._read_json_file(file_path_index)
            data_question_nros = self._read_json_file(file_path_data)

            if set([str(index) for index in index_question_nros ]) == set(data_question_nros['questions'].keys()):
                return True
            else:
                return False

