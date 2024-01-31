import os
import dotenv
import aiohttp
import asyncio

from abc import ABC , abstractmethod
from aiohttp import ClientTimeout
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from sessionmanager.session_manager import SessionManager
from etl.extract.extractquestions.question_payloads import QuestionPayloads

dotenv.load_dotenv()
GET_QUESTION_URL = os.getenv('GET_QUESTION_URL')
GET_QUESTION_ACTS_URL = os.getenv('GET_QUESTION_ACTS_URL')
GET_QUESTION_KEYWORDS_URL = os.getenv('GET_QUESTION_KEYWORDS_URL')

MAX_RETRIES = 3
RETRY_WAIT_SECONDS = 2


class ExtractQuestionsBase(ABC):

    def __init__(self, sessionManager: SessionManager):
        self.TIMEOUT = ClientTimeout(total=25)

        self.sessionManager = sessionManager
        self.payloads = QuestionPayloads()
            
    @retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    retry=(retry_if_exception_type((Exception,asyncio.TimeoutError))))
    async def get_question(self, question_nro: int) -> dict:
        '''
        For a given question_nro, returns the question data.
        '''

        qa_payload = self.payloads._get_question_payload(question_nro)
        request_url = GET_QUESTION_URL
        
        try:
            async with aiohttp.ClientSession(headers=self.sessionManager.get_headers(), cookies=self.sessionManager.get_cookies(), connector=aiohttp.TCPConnector(ssl=False), timeout=self.TIMEOUT) as session:
                async with session.post(request_url, json=qa_payload) as response:
                    data = await response.json()
            return data
        
        except asyncio.TimeoutError:
            print(f"Request {question_nro} timed out.")
            return None

    @retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    retry=(retry_if_exception_type((Exception,asyncio.TimeoutError))))
    async def get_question_acts(self, question_nro: int) -> dict:
        '''
        For a given question_nro, returns the acts associated with the question.
        '''

        url = GET_QUESTION_ACTS_URL
        payload = self.payloads._get_question_acts_payload(question_nro)

        async with aiohttp.ClientSession(headers=self.sessionManager.get_headers(), cookies=self.sessionManager.get_cookies(), connector=aiohttp.TCPConnector(ssl=False), timeout=self.TIMEOUT) as session:
            try:
                try:
                    async with session.post(url, json=payload) as response:
                        data = await response.json()

                except asyncio.TimeoutError:
                    print(
                        f"Request {question_nro} timed out. Continuing with the next request.")
                    return None
                return data
            
            except Exception as e:
                print(f"Exception: {e}")
                return None    
            
    @retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    retry=(retry_if_exception_type((Exception,asyncio.TimeoutError))))
    async def get_question_keywords(self, question_id: int) -> dict:
        '''
        For a given question_id, returns the keywords associated with the question.
        '''

        payload = self.payloads._get_question_keywords_payload(question_id)
        url = GET_QUESTION_KEYWORDS_URL

        async with aiohttp.ClientSession(headers=self.sessionManager.get_headers(), cookies=self.sessionManager.get_cookies(), connector=aiohttp.TCPConnector(ssl=False), timeout=self.TIMEOUT) as session:
            try:
                try:
                    async with session.post(url, json=payload) as response:
                        data = await response.json()

                except asyncio.TimeoutError:
                    print(
                        f"Request {question_id} timed out. Continuing with the next request.")
                    return None
                return data
            
            except Exception as e:
                print(f"Exception: {e}")
                return None
    
    @abstractmethod
    async def get_complete_question(self, question_nro: int) -> dict:
        '''
        For given question number, return the accociated question, acts and keywords.
        Should use get_question, get_question_acts and get_question_keywords.
        '''
        pass

    @abstractmethod
    async def _get_max_hits(self, domains: list[dict] = None) -> int:
        '''
        For a given list of domains, returns the total number of questions within those domains.
        '''
        pass

    @abstractmethod
    async def _get_question_nros_range(self, start_from: int = 0, batch_size: int = 25 , domains: list[dict] = None) -> list[int]:
        '''
        For a given list of domains, returns the question numbers from start_from to start_from + batch_size.
        '''
        pass

    @abstractmethod
    async def _get_question_nros_all(self, domains: list[dict] = None) -> list[int]:
        '''
        For a given list of domains, returns all question numbers.
        It should use get_question_nros_range to get the question numbers in batches.
        '''
        pass
    @abstractmethod
    async def get_all_questions(self, domains:list[dict] = None, file: str = None) -> dict or None:
        '''
        Given domains, use get_question_nros_all to get all question numbers.
        Then use get_complete_question to get the question data for each question number.
        If file is not None, save the results to a file else return the results.
        '''
        pass