import os
import tqdm
import json
import dotenv
import asyncio
import aiohttp
import logging

from aiohttp import ClientTimeout
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from sessionmanager.session_manager import SessionManager
from etl.extract.extractquestions.question_payloads import QuestionPayloads
from etl.extract.extractquestions.question_parser import QuestionParser
from etl.extract.extractquestions.extract_questions_base import ExtractQuestionsBase

from etl.common.questionindex.raw_question_index import RawQuestionIndex

dotenv.load_dotenv()
DOMAINS = os.getenv('DOMAINS')
GET_REQUEST_URL = os.getenv('GET_REQUEST_URL')


class ExtractQuestionsDomain(ExtractQuestionsBase):
    def __init__(self, sessionManager: SessionManager):
        self.BATCH_SIZE = 25
        self.REQUEST_URL = GET_REQUEST_URL
        self.TIMEOUT = ClientTimeout(total=25)
        self.domains = json.loads(DOMAINS)

        self.sessionManager = sessionManager
        self.payloads = QuestionPayloads()
        self.parser = QuestionParser()
        self.index = RawQuestionIndex()

        super().__init__(sessionManager)
    
    @retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=(retry_if_exception_type(asyncio.TimeoutError)))
    async def _get_max_hits(self, domains: list[dict] = None) -> int:
        '''
        Return the total number of questions and answers within the given domains.
        '''

        async with aiohttp.ClientSession(headers=self.sessionManager.get_headers(), cookies=self.sessionManager.get_cookies(), connector=aiohttp.TCPConnector(ssl=False), timeout=self.TIMEOUT) as session:
            search_payload = self.payloads._get_question_search_payload(domains=domains)
            try:
                response = await session.post(self.REQUEST_URL, json=search_payload, timeout=self.TIMEOUT)
            except asyncio.TimeoutError:
                logging.warning(f"Request for max_hits timed out. Continuing with the next request.")
                return None
            data = await response.json()
            if data.get('availableHitCount') is None:
                logging.warning('No availableHitCount in response. Continuing with the next request.')
                logging.debug(data)
                return None
            else:
                return data['availableHitCount']

    @retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=(retry_if_exception_type(asyncio.TimeoutError)))
    async def _get_question_nros_range(self, start_from: int = 0, batch_size: int = 25, domains:list[dict] = None)-> list[int]:
        '''
        Return the question Id's of questions within the given domains from range start_from to start_from+batch_size.
        '''

        async with aiohttp.ClientSession(headers=self.sessionManager.get_headers(), cookies=self.sessionManager.get_cookies(), connector=aiohttp.TCPConnector(ssl=False), timeout=self.TIMEOUT) as session:
            search_payload = self.payloads._get_question_search_payload(start_from=start_from,batch_size=batch_size,domains=domains)

            question_nros = []

            try:
                response = await session.post(self.REQUEST_URL, json=search_payload, timeout=self.TIMEOUT)
            except asyncio.TimeoutError:
                logging.warning(f"Request nro timed out. Continuing with the next request.")
                return []

            data = await response.json()

            if data.get('documentList') is None:
                logging.warning('No documentList in response. Continuing with the next request.')
                logging.debug(data)
                return []
            else:
                for question in data['documentList']:
                    question_nros.append(question['nro'])
            return question_nros
    
    @retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=(retry_if_exception_type(asyncio.TimeoutError)))
    async def _get_question_nros_all(self, domains: list[dict] = None) -> list[list[int]]:
        '''
        Return the question Id's of questions and answers within the given domains.
        '''
        max_hits = await self._get_max_hits(domains=domains)
        total_calls = max_hits // self.BATCH_SIZE
        remainder = max_hits % self.BATCH_SIZE

        semaphore = asyncio.Semaphore(60)

        async def limited_search(start_from, batch_size, domains=domains):
            async with semaphore:
                return await self._get_question_nros_range(start_from=start_from, batch_size=batch_size, domains=domains)

        tasks = [limited_search(i * self.BATCH_SIZE,
                                self.BATCH_SIZE if i < total_calls else remainder,domains=domains)
                 for i in range(total_calls + (1 if remainder else 0))]
        
        results = await asyncio.gather(*tasks)

        nro_list=[item for sublist in results for item in sublist]

        missing_nros = self.index._find_missing_nros(nro_list=nro_list,batch_size=self.BATCH_SIZE,domains=domains )
        
        self.index._update_questions_index(nro_list=nro_list,domains=domains)
        
        return missing_nros
    
    @retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=(retry_if_exception_type(asyncio.TimeoutError)))
    async def get_complete_question(self, question_nro: int) -> dict:
        '''
        For a given question_nro, returns the question data, acts and keywords associated with the question.
        '''
        question = await self.get_question(question_nro)
        question_id = question['id']

        results = await asyncio.gather(self.get_question_acts(question_nro), self.get_question_keywords(question_id))

        acts = results[0]
        keywords = results[1]

        complete_question = self.parser.parse_question_data(question, acts, keywords)

        return complete_question
    

    async def get_all_questions(self, domains:list[dict] = None) -> list[str]:
        '''
        Retrieve all questions, associated keywords and acts from the given domains or from every domain if None.
        Returns a list of question_nros that were successfully retrieved and indexed.
        '''
        question_tasks = []
        
        results = await self._get_question_nros_all(domains=domains)

        for result in results:
            question_tasks.append([self.get_complete_question(question_nro=nro) for nro in result])

        questions = []
        
        for qa_task in tqdm.tqdm(question_tasks):
            qa_results = await asyncio.gather(*qa_task)
            for qa_result in qa_results:
                if qa_result is not None:
                    questions.append(qa_result)
        
        if len(questions) == 0:
            return []
        else:
            result_nros = self.index._update_questions_data(questions=questions,domains=domains)
        return result_nros
        