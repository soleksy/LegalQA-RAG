import os
import dotenv
import aiohttp
import asyncio

from abc import ABC
from aiohttp import ClientTimeout
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from sessionmanager.session_manager import SessionManager
from extract.extractquestions.question_payloads import QuestionPayloads

dotenv.load_dotenv()
GET_QUESTION_URL = os.getenv('GET_QUESTION_URL')



class ExtractQuestionsBase(ABC):

    def __init__(self, sessionManager: SessionManager):
        self.TIMEOUT = ClientTimeout(total=25)

        self.sessionManager = sessionManager
        self.payloads = QuestionPayloads()
            
    @retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=(retry_if_exception_type((Exception,asyncio.TimeoutError))))
    async def get_question(self, question_nro: int) -> dict:
        '''
        For a given question_nro, returns the question data.
        '''

        qa_payload = self.payloads.get_question_payload(question_nro)
        request_url = GET_QUESTION_URL
        
        try:
            async with aiohttp.ClientSession(headers=self.sessionManager.get_headers(), cookies=self.sessionManager.get_cookies(), connector=aiohttp.TCPConnector(ssl=False), timeout=self.TIMEOUT) as session:
                async with session.post(request_url, json=qa_payload) as response:
                    data = await response.json()
            return data
        
        except asyncio.TimeoutError:
            print(f"Request {question_nro} timed out.")
            return None
    
