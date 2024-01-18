import os
import tqdm
import dotenv
import aiohttp
import asyncio
import logging
import datetime
import requests

from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type,wait_exponential
from bs4 import BeautifulSoup

from sessionmanager.session_manager import SessionManager
from models.datamodels.tree_act import TreeAct, RelatedKeyword
from etl.common.actindex.tree_act_index import TreeActIndex
from etl.common.questionindex.transformed_question_index import TransformedQuestionIndex
from etl.extract.extractacts.act_payloads import ActPayloads
from etl.extract.extractacts.act_parser import ActParser

dotenv.load_dotenv()
GET_ACT_KEYWORDS_URL = os.getenv('GET_ACT_KEYWORDS_URL')
GET_ACT_BASE_URL = os.getenv('GET_ACT_BASE_URL')

GET_LINK_BASE_URL = os.getenv('GET_LINK_BASE_URL')
GET_LINK_TO_AVOID = os.getenv('GET_LINK_TO_AVOID')
GET_CITE_BASE_URL = os.getenv('GET_CITE_BASE_URL')

logging.basicConfig(level=logging.WARNING)

class RetryableHTTPError(Exception):
        """Custom exception class for retryable HTTP errors."""
        pass

class ExtractActs():
    
    def __init__(self, sessionManager: SessionManager):
        self.sessionManager = sessionManager
        self.tree_acts_index = TreeActIndex()
        self.transformed_question_index = TransformedQuestionIndex()
        self.payloads = ActPayloads()
        self.parser = ActParser()

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

    @retry(retry=retry_if_exception_type(RetryableHTTPError), wait=wait_exponential(min=1, max=60))
    async def get_pagination_results(self, session, url, class_name):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.text()
                    soup = BeautifulSoup(content, 'html.parser')
                    spans_result = [span.text for div in soup.find_all('div', class_=class_name) for span in div.find_all('span')]
                    return int(spans_result[-2])
                else:
                    return f"Failed with status code: {response.status}"
        except aiohttp.ClientResponseError as e:
            if e.status == 500:
                logging.error(f"HTTP 500 error for URL: {url}. Retrying...")
                raise RetryableHTTPError(f"HTTP 500 error for URL: {url}")
            else:
                logging.error(f"HTTP error {e.status} for URL: {url}")
                return f"Failed with status code: {e.status}"
        except Exception as e:
            logging.error(f"Error in get_pagination_results: {e}")
            raise


    @retry(retry=retry_if_exception_type(RetryableHTTPError), wait=wait_exponential(min=1, max=60))
    async def get_links_with_exact_class(self,session,url, class_name):
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                content = await response.text()
                soup = BeautifulSoup(content, 'html.parser')
                links = soup.find_all('a', class_=class_name)

                nro_to_link = {}
                for link in links:
                    if link.get('href') and GET_LINK_TO_AVOID not in link.get('href'):
                        nro = link.get('href').split('-')[-1]
                        if nro not in nro_to_link: 
                            nro_to_link[nro] = GET_LINK_BASE_URL + link.get('href') + '/'

                return nro_to_link
            
        except aiohttp.ClientResponseError as e:
            if e.status == 500:
                logging.error(f"HTTP 500 error for URL: {url}. Retrying...")
                raise RetryableHTTPError(f"HTTP 500 error for URL: {url}")
            else:
                logging.error(f"HTTP error {e.status} for URL: {url}")
                return {}  
        except Exception as e:
            logging.error(f"Error in get_links_with_exact_class: {e}")
            return {}  

    async def get_all_links(self , base_url):
        async with aiohttp.ClientSession() as session:
            pagination_class_name = "pagination-results"
            total_acts_count = await self.get_pagination_results(session, base_url + '1', pagination_class_name)
            logging.info(f'Total acts: {total_acts_count}')

            per_page = 90
            pages_count = (total_acts_count + per_page - 1) // per_page

            batch_size = 25
            all_dicts = {}

            for i in tqdm.tqdm(range(1, pages_count + 1, batch_size)):
                end = min(i + batch_size, pages_count + 1)
                batched_tasks = [self.get_links_with_exact_class(session, base_url + str(page), "wk-link") for page in range(i, end)]
                results = await asyncio.gather(*batched_tasks)
                for result in results:
                    all_dicts.update(result)

            return all_dicts
        
    @retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=(retry_if_exception_type(Exception)))
    async def get_act(self, act_nro: int , link: str) -> TreeAct:
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

        
        data['citeLink'] = link
        return self.parser.parse_single_act(html_content=response.text, tree_act=TreeAct(**data), data=data)
    
    async def get_acts(self, act_nros: list[int]) -> None:
        '''
        Extract all acts from the list of act_nros and save them to the index.
        '''
        logging.info("Extracting links...")
        links = await self.get_all_links(base_url=GET_CITE_BASE_URL)
        not_indexed_acts = self._find_not_indexed_acts(act_nros=act_nros)
        self.tree_acts_index._update_act_index(not_indexed_acts)
        logging.info("Extracting acts...")
        for act_nro in tqdm.tqdm(not_indexed_acts):
            tree_act = await self.get_act(act_nro=act_nro, link=links[str(act_nro)])
            
            file_name = self.tree_acts_index._get_filename_data(act_nro)
            file_path = self.tree_acts_index.tree_acts_data_path+file_name

            if os.path.exists(file_path):
                logging.warning(f"Act with id {act_nro} already exists.")
                continue
            else:
                self.tree_acts_index._write_json_file(file_path, tree_act.model_dump())
        
        