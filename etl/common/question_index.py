import os
import json
import datetime

from etl.common.index import Index

class QuestionIndex(Index):
    def __init__(self) -> None:
        super().__init__()
        self.raw_questions_index_path = self.config['raw_questions_index']
        self.raw_questions_data_path = self.config['raw_questions_data']


    def _get_filename_index(self, domains: list[dict] = None) -> str:
        '''
        Given a list of domains, return the filename of the index file.
        '''

        if domains is not None:
            return '_'.join(str(list(d.values())[0]) for d in domains) + '_index.json'
        else:
            return 'all_index.json'
    
    def _get_filename_data(self, domains: list[dict] = None) -> str:
        '''
        Given a list of domains, return the filename of the data file.
        '''

        if domains is not None:
            return '_'.join(str(list(d.values())[0]) for d in domains) + '_data.json'
        else:
            return 'all_data.json'
        

    def _find_missing_nros(self, nro_list: list[int] ,batch_size , domains: list[dict] = None) -> bool:
        '''
        Given a list of question_nros, check if the index of raw questions exists and return only not indexed nros.
        '''

        file_name = self._get_filename_index(domains=domains)

        if os.path.exists(self.raw_questions_index_path+file_name):
            with open(self.raw_questions_index_path+file_name) as f:
                index = json.load(f)

            missing = list(set(nro_list).difference(set(index)))

            if len(missing) > 0:
                missing = [missing[i:i + batch_size] for i in range(0, len(missing), batch_size)]
                print(missing)
                return missing
            else:
                return []
        else:
            return [nro_list[i:i + batch_size] for i in range(0, len(nro_list), batch_size)]
        
    def _update_raw_questions_index(self, nro_list: list[int] , domains: list[dict] = None) -> None:
        '''
        Given a list of question_nros, create or update the index of raw questions.
        '''

        file_name = self._get_filename_index(domains=domains)

        if os.path.exists(self.raw_questions_index_path+file_name):
            with open(self.raw_questions_index_path+file_name) as f:
                index = json.load(f)

            nro_list = list(set(nro_list + index))

            with open(self.raw_questions_index_path+file_name, 'w') as f:
                json.dump(nro_list, f , indent=4 , ensure_ascii=False)
        else:
            with open(self.raw_questions_index_path+file_name, 'w') as f:
                json.dump(nro_list, f , indent=4 , ensure_ascii=False)

    def _update_raw_questions_data(self, questions: list[dict] , domains: list[dict] = None) -> list[str]:
        '''
        Given a list of questions, create or update the raw questions data.
        '''

        file_name = self._get_filename_data(domains=domains)
        result_nros = []

        if os.path.exists(self.raw_questions_data_path+file_name):
            with open(self.raw_questions_data_path+file_name) as f:
                data = json.load(f)

            question_keys = set(data['questions'].keys())

            for question in questions:
                if str(question['nro']) not in question_keys:
                    result_nros.append(question['nro'])
                    data['questions'][question['nro']] = question
                else:
                    print(f"Question with nro {question['nro']} already exists in data file.")

            with open(self.raw_questions_data_path+file_name, 'w') as f:
                json.dump(data, f , indent=4 , ensure_ascii=False)
        else:
            with open(self.raw_questions_data_path+file_name, 'w') as f:
                result_nros = [str(question['nro']) for question in questions]
                json.dump({'last_update': str(datetime.datetime.now()).split()[0], 'questions': {question['nro']: question for question in questions}}, f , indent=4 , ensure_ascii=False)

        return result_nros

    def _validate_index(self, domains: list[dict] = None) -> bool:
        '''
        Given a list of domains, check if the set of question_nros in the index is equal to the set of question_nros in the data.
        '''

        file_name_index = self._get_filename_index(domains=domains)
        file_name_data = self._get_filename_data(domains=domains)

        if os.path.exists(self.raw_questions_index_path+file_name_index) and os.path.exists(self.raw_questions_data_path+file_name_data):
            with open(self.raw_questions_index_path+file_name_index) as f:
                index_question_nros = json.load(f)

            with open(self.raw_questions_data_path+file_name_data) as f:
                data_question_nros = json.load(f)

            if set([str(index) for index in index_question_nros ]) == set(data_question_nros['questions'].keys()):
                return True
            else:
                return False