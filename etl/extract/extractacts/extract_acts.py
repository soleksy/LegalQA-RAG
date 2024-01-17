import os
import dotenv
import logging
import datetime
import requests

from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from sessionmanager.session_manager import SessionManager
from models.datamodels.tree_act import TreeAct, RelatedKeyword
from etl.common.actindex.tree_act_index import TreeActIndex
from etl.common.questionindex.transformed_question_index import TransformedQuestionIndex
from etl.extract.extractacts.act_payloads import ActPayloads

dotenv.load_dotenv()
GET_ACT_KEYWORDS_URL = os.getenv('GET_ACT_KEYWORDS_URL')
GET_ACT_BASE_URL = os.getenv('GET_ACT_BASE_URL')

class ExtractActs():
    
    def __init__(self, sessionManager: SessionManager):
        self.sessionManager = sessionManager
        self.tree_acts_index = TreeActIndex()
        self.transformed_question_index = TransformedQuestionIndex()
        self.payloads = ActPayloads()

    def _find_not_indexed_acts(self, act_nros: list[int]) -> list[int]:
        '''
        Given a list of act_nros, check if the index of transformed acts exists and return only not transformed nros.
        '''
        file_name = self.tree_acts_index._get_filename_index()
        file_path = self.tree_acts_index.tree_acts_index_path+file_name

        if os.path.exists(file_path):
            return self.tree_acts_index._find_missing_nros(nro_list=act_nros)
        else:
            return act_nros
    
    @retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=(retry_if_exception_type(Exception)))
    def _get_act_keywords(self, act_id: int) -> list[dict]:
        '''
        Return keywords for any legal act based on its id
        '''
        request_url = GET_ACT_KEYWORDS_URL
        payload = self.payloads.get_act_keywords_payload(act_id)
        try:
            response = requests.post(request_url,
                                    headers=self.sessionManager.get_headers(),
                                    cookies=self.sessionManager.get_cookies(),
                                    json=payload)
        except Exception as e:
            logging.error(f"Exception: {e}, {act_id}")
        data = response.json()
        if 'keywords' not in data.keys():
            logging.warning(f"Could not find keywords for act with id: {act_id}")
            return None
        return data['keywords']
    
    @retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=(retry_if_exception_type(Exception)))
    def get_act(self, act_nro: int) -> TreeAct:
        '''
        Return any legal act based on its id
        '''

        date = str(datetime.datetime.now()).split()[0] 
        request_url = f'{GET_ACT_BASE_URL}?nro={act_nro}&pointInTime={date}'
        try:
            response = requests.get(request_url,
                                    headers=self.sessionManager.get_headers(),
                                    cookies=self.sessionManager.get_cookies())
        except Exception as e:
            logging.error(f"Exception: {e}, {act_nro}")
        data = response.json()

        if 'actLawType' not in data.keys():
            data['actLawType'] = None

        if 'title' not in data.keys():
            data['title'] = None

        if 'shortQuote' not in data.keys():
            data['shortQuote']=None

        id = data['id']

        keywords = self._get_act_keywords(id)
        keywords_models = []

        for keyword in keywords:
            keywords_models.append(RelatedKeyword(**keyword))

        data['keywords'] = keywords_models
        data['elements'] = {}

        return TreeAct(**data)

