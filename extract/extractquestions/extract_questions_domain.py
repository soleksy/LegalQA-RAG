
import os
import json
import dotenv
import asyncio
import aiohttp
import logging

from aiohttp import ClientTimeout
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from sessionmanager.session_manager import SessionManager
from extract.extractquestions.question_payloads import QuestionPayloads
from extract.extractquestions.extract_questions_base import ExtractQuestionsBase

dotenv.load_dotenv()
DOMAINS = os.getenv('DOMAINS')
GET_REQUEST_URL = os.getenv('GET_REQUEST_URL')


class ExtractQuestionsDomain(ExtractQuestionsBase):
    def __init__(self, sessionManager: SessionManager):
        self.TIMEOUT = ClientTimeout(total=25)
        self.BATCH_SIZE = 25
        self.REQUEST_URL = GET_REQUEST_URL
        self.sessionManager = sessionManager
        self.domains = json.loads(DOMAINS)
        self.payloads = QuestionPayloads()

        super().__init__(sessionManager)
    
    @retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=(retry_if_exception_type(asyncio.TimeoutError)))
    async def get_max_hits(self, domains: list[dict] = None) -> int:
        '''
        Return the total number of questions and answers within the given domains.
        '''

        async with aiohttp.ClientSession(headers=self.sessionManager.get_headers(), cookies=self.sessionManager.get_cookies(), connector=aiohttp.TCPConnector(ssl=False), timeout=self.TIMEOUT) as session:
            search_payload = self.payloads.get_question_search_payload(domains=domains)
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
