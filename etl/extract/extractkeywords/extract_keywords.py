import os
import tqdm
import json
import aiohttp
import asyncio
import logging
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from sessionmanager.session_manager import SessionManager
from etl.extract.extractkeywords.keyword_payloads import KeywordPayloads
from etl.common.questionindex.transformed_question_index import TransformedQuestionIndex
from etl.common.keywordindex.raw_keyword_index import RawKeywordIndex

logging.basicConfig(level=logging.WARNING)
load_dotenv()

GET_KEYWORD_URL = os.getenv('GET_KEYWORD_URL')
BATCH_SIZE = 25
MAX_RETRIES = 3
RETRY_WAIT_SECONDS = 2

class ExtractKeywords:
    def __init__(self, session_manager: SessionManager):
        self.session_manager = session_manager
        self.transformed_question_index = TransformedQuestionIndex()
        self.raw_keyword_index = RawKeywordIndex()
        self.keyword_payloads = KeywordPayloads()
        self.timeout = aiohttp.ClientTimeout(total=20) 

    def _find_missing_keywords(self, keywords: list[dict]) -> list[dict]:
        return self.raw_keyword_index._find_missing_keywords(keywords)
    
    def _create_session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            headers=self.session_manager.get_headers(),
            cookies=self.session_manager.get_cookies(),
            connector=aiohttp.TCPConnector(ssl=False),
            timeout=self.timeout
        )

    def _parse_single_keyword(self, data: dict) -> list[dict]:
        document_list = data.get('documentList', [])
        if not document_list:
            logging.warning('No document data for keyword')
            return []
        return [
            {
                'title': document['title'],
                'nro': document['nro'],
                'lawType': document['lawType'],
                'validity': document['validity'],
                'relationData': document['relationData']
            }
            for document in document_list
        ]


    @retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    retry=(retry_if_exception_type(asyncio.TimeoutError)))
    async def _get_max_hits(self, keyword_id:int , ui_concept_id:int=-1) -> int:
        '''
        Return the total number of related data for a given keyword_id and ui_concept_id.
        '''
        payload = self.keyword_payloads._get_keyword_payload(keyword_id=keyword_id,ui_concept_id=ui_concept_id)
        
        async with self._create_session() as session:
            try:
                response = await session.post(GET_KEYWORD_URL, json=payload ,timeout=self.timeout)
            except asyncio.TimeoutError:
                logging.warning(f"Request {keyword_id} timed out. Continuing with the next request.")
                return None
            data = await response.json()

            if data.get('availableHitCount') is None:
                logging.warning('No availableHitCount in response. Continuing with the next request.')
                logging.debug(data)
                return None
            else:
                return data['availableHitCount']

    @retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    retry=(retry_if_exception_type(asyncio.TimeoutError)))
    async def _get_keyword_part(self, keyword_id: int, start_from: int = 0, batch_size: int = BATCH_SIZE, ui_concept_id: int = -1) -> list[dict]:
        payload = self.keyword_payloads._get_keyword_payload(keyword_id=keyword_id,ui_concept_id=ui_concept_id)
        try:
            async with self._create_session() as session:
                async with session.post(url=GET_KEYWORD_URL, json=payload) as response:
                    data = await response.json()
                    return self._parse_single_keyword(data=data)
        except asyncio.TimeoutError:
            logging.warning(f"Request {keyword_id} timed out. Continuing with the next request.")
        except Exception as e:
            logging.error(f"Exception occurred: {e}", exc_info=True)
        return []


    def _concatenate_results(self, results: list) -> dict:
        if not results:
            return {}
        return {element['nro']: element for sublist in results if sublist for element in sublist}

    def _write_json_to_file(self, file_path: str, data: dict) -> None:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

    def _create_tasks(self, keyword_id: int, max_hits: int, ui_concept_id: int) -> list:
        tasks = []
        total_calls = (max_hits + BATCH_SIZE - 1) // BATCH_SIZE  # Calculate total calls including possible remainder

        for i in range(total_calls):
            batch_size = BATCH_SIZE if i < total_calls - 1 else max_hits % BATCH_SIZE or BATCH_SIZE
            tasks.append(self._get_keyword_part(keyword_id=keyword_id, start_from=i * BATCH_SIZE, batch_size=batch_size, ui_concept_id=ui_concept_id))

        return tasks

    @retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    retry=(retry_if_exception_type(asyncio.TimeoutError)))
    async def _wrapped_task(self, task, pbar) -> dict[str, dict]:
        result = await task
        pbar.update(1)
        return result

    async def _run_tasks(self, keywords) -> None:
        tasks = []

        semaphore = asyncio.Semaphore(10)
        with tqdm.tqdm(total=len(keywords)) as pbar:
            for keyword in keywords:
                task = self._get_keyword(keyword_data=keyword , semaphore=semaphore)
                wrapped = self._wrapped_task(task, pbar)
                tasks.append(wrapped)

            await asyncio.gather(*tasks)

    @retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    retry=(retry_if_exception_type(asyncio.TimeoutError)))
    async def _get_keyword(self, keyword_data: dict , semaphore: asyncio.Semaphore) -> None:

        keyword_id = keyword_data['conceptId']
        ui_concept_id = keyword_data['instanceOfType']

        max_hits = await self._get_max_hits(keyword_id=keyword_id, ui_concept_id=ui_concept_id)

        if max_hits == 0:
            folder_path = self.raw_keyword_index.raw_keyword_data_path
            self._write_json_to_file(f"{folder_path}/{keyword_id}_({ui_concept_id}).json", [])
            return

        tasks = self._create_tasks(keyword_id, max_hits, ui_concept_id)

        try:
            async with semaphore:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                concatenated_results = self._concatenate_results(results)

                if not concatenated_results:
                    logging.warning(f"Could not find keyword with id: {keyword_id}")

                folder_path = self.raw_keyword_index.raw_keyword_data_path
                if folder_path:
                    self._write_json_to_file(f"{folder_path}/{keyword_id}_({ui_concept_id}).json", concatenated_results)
        except Exception as e:
            logging.error(f"Error gathering keyword parts: {e}", exc_info=True)

    async def get_keywords(self, keywords: list[dict]) -> None:
        keywords = self._find_missing_keywords(keywords)

        await self._run_tasks(keywords)

        if keywords:
            self.raw_keyword_index._update_keyword_index(keywords)
    
