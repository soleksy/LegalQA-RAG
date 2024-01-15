import os
import dotenv
import datetime
import requests

from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from sessionmanager.session_manager import SessionManager
from etl.common.transformed_question_index import TransformedQuestionIndex
from etl.common.raw_question_index import RawQuestionIndex
from models.datamodels.question import Question


dotenv.load_dotenv()

UNITS_BASE_URL= os.getenv('UNITS_BASE_URL')


class TransformQuestions():
    '''
    Class for transforming raw question data.
    '''
    def __init__(self , sessionManager: SessionManager):
        self.sessionManager = sessionManager

        self.transformed_index = TransformedQuestionIndex()
        self.raw_index = RawQuestionIndex()

        self.raw_questions_index_path = self.raw_index.raw_questions_index_path
        self.raw_questions_data_path =  self.raw_index.raw_questions_data_path

        self.transformed_questions_index_path = self.transformed_index.transformed_questions_index_path
        self.transformed_questions_data_path = self.transformed_index.transformed_questions_data_path

    def _find_not_indexed_questions(self, question_nros: list[str], domains: list[dict] = None) -> list[str]:
        '''
        Given a list of question_nros, check if the index of transformed questions exists and return only not transformed nros.
        '''
        file_name = self.transformed_index._get_filename_data_transformed(domains=domains)
        file_path = self.transformed_questions_data_path+file_name

        if os.path.exists(file_path):
            return self.transformed_index._find_missing_nros_transformed(nro_list=question_nros, domains=domains)
        else:
            return question_nros

    def _retrieve_not_indexed_questions(self, question_nros: list[str] , domains: list[dict] = None) -> list[Question]:
        '''
        Given a list of question nros to transform, retrieve them from the raw data, transform them and save them.
        '''
        raw_filename = self.raw_index._get_filename_data_raw(domains=domains)
        raw_questions = self.raw_index._read_json_file(self.raw_questions_data_path+raw_filename)
        questions_to_transform = [raw_questions['questions'][nro] for nro in question_nros]

        return [Question(**question) for question in questions_to_transform]
    

    @retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=(retry_if_exception_type(Exception)))
    def _is_parsable(self, nro: int) -> bool:
        '''
        Returns True if the act is parsable, False otherwise.
        '''
        date = str(datetime.datetime.now()).split()[0]

        request_url = f'{UNITS_BASE_URL}?nro={nro}&pointInTime={date}'

        try:
            response = requests.get(request_url,
                                    headers=self.sessionManager.get_headers(),
                                    cookies=self.sessionManager.get_cookies())
        except Exception as e:
            print(f"Exception: {e}, {nro}")
        data = response.json()

        if 'units' not in data.keys():
            return False
        else:
            return True
        
    def _transform_questions(self, question_nros: list[str], domains: list[dict] = None) -> list[Question]:

        question_nros = self._find_not_indexed_questions(question_nros=question_nros, domains=domains)
        
        questions_to_transform = self._retrieve_not_indexed_questions(question_nros=question_nros, domains=domains)


        parsable = set()
        unparsable = set()

        related_acts = set([relatedAct.nro for question in questions_to_transform for relatedAct in question.relatedActs])

        for nro in related_acts:
            if nro not in parsable and nro not in unparsable:
                if self._is_parsable(nro):
                    parsable.add(nro)
                else:
                    unparsable.add(nro)

        transformed_questions = []
        for question in questions_to_transform:
            transformed = self._transform_question(question, parsable=parsable)
            if transformed is not None:
                transformed_questions.append(transformed)

        #Index the transformed questions
        self.transformed_index._update_questions_index_transformed(nro_list=question_nros, domains=domains)
        transformed_questions = self.transformed_index._update_questions_data_transformed(questions=transformed_questions, domains=domains)
        
        return transformed_questions
    
    def _transform_question(self, question: Question , parsable: set) -> dict:
        '''
        Transform a single question.
        '''
        
        #Remove not actual acts
        question.relatedActs = [relatedAct for relatedAct in question.relatedActs if relatedAct.validity == 'ACTUAL']
        #Remove changing acts
        question.relatedActs = [relatedAct for relatedAct in question.relatedActs if (not relatedAct.title.startswith('Zmiana ') and not relatedAct.title.startswith('Zm.: '))]
        #Remove acts that are not parsable
        question.relatedActs = [relatedAct for relatedAct in question.relatedActs if relatedAct.nro in parsable]

        #Clean RelationsData
        prefixes = ('all', 'ks', 'dz', 'roz', 'tyt')
        for relatedAct in question.relatedActs:
            if relatedAct.relationData is not None:
                relatedAct.relationData = [citationData for citationData in relatedAct.relationData if not citationData.id.startswith(prefixes)]
            else:
                relatedAct.relationData = []
        
        if question.keywords == []:
            return None
        if question.relatedActs == []:
            return None

        return question.model_dump()
    