import os
import json
import dotenv
import asyncio
import aiohttp
import logging
import datetime

from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from sessionmanager.session_manager import SessionManager
from etl.common.questionindex.transformed_question_index import TransformedQuestionIndex
from etl.common.questionindex.raw_question_index import RawQuestionIndex
from models.datamodels.question import Question


dotenv.load_dotenv()

UNITS_BASE_URL= os.getenv('UNITS_BASE_URL')

MAX_RETRIES = 3
RETRY_WAIT_SECONDS = 2

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
        file_name = self.transformed_index._get_filename_data(domains=domains)
        file_path = self.transformed_questions_data_path+file_name

        if os.path.exists(file_path):
            return self.transformed_index._find_missing_nros(nro_list=question_nros, domains=domains)
        else:
            return question_nros

    def _retrieve_not_indexed_questions(self, question_nros: list[str] , domains: list[dict] = None) -> list[Question]:
        '''
        Given a list of question nros to transform, retrieve them from the raw data, transform them and save them.
        '''
        raw_filename = self.raw_index._get_filename_data(domains=domains)
        raw_questions = self.raw_index._read_json_file(self.raw_questions_data_path+raw_filename)
        questions_to_transform = [raw_questions['questions'][nro] for nro in question_nros]

        return [Question(**question) for question in questions_to_transform]
    

    @retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    retry=(retry_if_exception_type(asyncio.TimeoutError)))
    async def _is_parsable(self, nro: int) -> bool:
        '''
        Returns True if the act is parsable, False otherwise.
        '''
        date = str(datetime.datetime.now()).split()[0]
        request_url = f'{UNITS_BASE_URL}?nro={nro}&pointInTime={date}'

        async with aiohttp.ClientSession(headers=self.sessionManager.get_headers(), cookies=self.sessionManager.get_cookies(), connector=aiohttp.TCPConnector(ssl=False)) as session:
            try:
                response = await session.get(request_url)
            except asyncio.TimeoutError:
                logging.warning(f"Request for parsability timed out. Continuing with the next request.")
                return False
            data = await response.json()

            if 'units' not in data.keys():
                return False
            else:
                return True
        
    async def transform_questions(self, question_nros: list[str], domains: list[dict] = None) -> list[Question]:


        if question_nros == []:
            return []
        
        question_nros = self._find_not_indexed_questions(question_nros=question_nros, domains=domains)
        
        questions_to_transform = self._retrieve_not_indexed_questions(question_nros=question_nros, domains=domains)


        parsable = set()
        unparsable = set()

        related_acts = list(set([relatedAct.nro for question in questions_to_transform for relatedAct in question.relatedActs]))
        related_acts = [related_acts[i:i + 25] for i in range(0, len(related_acts), 25)]
        
        tasks = [[self._is_parsable(nro) for nro in sublist] for sublist in related_acts]

        for n , task in enumerate(tasks):
            results = await asyncio.gather(*task)
            for result, nro in zip(results, related_acts[n]):
                if nro not in parsable and nro not in unparsable:
                    if result:
                        parsable.add(nro)
                    else:
                        unparsable.add(nro)

        transformed_questions = []
        for question in questions_to_transform:
            transformed = self._transform_question(question, parsable=parsable)
            if transformed is not None:
                transformed_questions.append(transformed)

        transformed_questions = self._get_only_frequent_acts_questions(n=100) #Smaller acts contain alot of api errors
        question_nros = [int(nro) for nro in transformed_questions.keys()]
        #Index the transformed questions
        self.transformed_index._update_questions_index(nro_list=question_nros, domains=domains)
        self.transformed_index._update_questions_data(questions=transformed_questions, domains=domains)
        
        transformed_questions = [Question(**question) for question in transformed_questions]

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
    
    def _get_act_frequencies(self) -> dict:
        '''
        Return a dictionary with act_nro as keys and frequency in questions as values.
        '''
        file_name = self.transformed_index._get_filename_data()
        file_path = self.transformed_questions_data_path+file_name

        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                data = json.load(f)
                questions = data['questions']
            
            act_frequencies = {}

            for question in questions:
                for relatedAct in questions[question]['relatedActs']:
                    if relatedAct['nro'] not in act_frequencies.keys():
                        act_frequencies[relatedAct['nro']] = 1
                    else:
                        act_frequencies[relatedAct['nro']] += 1

            #sort
            act_frequencies = dict(sorted(act_frequencies.items(), key=lambda item: item[1], reverse=True))

            return act_frequencies
    
    def _get_only_frequent_acts_questions(self, n: int = 100) -> list[int]:
        '''
        Return a list of questions with acts with at least n questions associated.
        '''
        act_frequencies = self._get_act_frequencies()
        top_n = [int(key) for key in act_frequencies.keys() if act_frequencies[key] >= n]

        file_name = self.transformed_index._get_filename_data()
        file_path = self.transformed_questions_data_path+file_name

        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                data = json.load(f)
                questions = data['questions']
        clean_questions = {}

        
        for question in questions:
            clean_related_acts = []
            for relatedAct in questions[question]['relatedActs']:
                if relatedAct['nro'] in top_n:
                    clean_related_acts.append(relatedAct)
            if clean_related_acts != []:
                clean_questions[question] = questions[question]
                clean_questions[question]['relatedActs'] = clean_related_acts
        
        return clean_questions

