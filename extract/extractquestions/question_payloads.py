import os
import json
import dotenv
import datetime


dotenv.load_dotenv()
GET_QUESTION_PAYLOAD=os.getenv('GET_QUESTION_PAYLOAD')
GET_QUESTION_ACTS_RELATIONSHIP_PAYLOAD=os.getenv('GET_QUESTION_ACTS_RELATIONSHIP_PAYLOAD')
GET_QUESTION_KEYWORDS_PAYLOAD=os.getenv('GET_QUESTION_KEYWORDS_PAYLOAD')
GET_QUESTION_SEARCH_PAYLOAD=os.getenv('GET_QUESTION_SEARCH_PAYLOAD')

class QuestionPayloads():
    def __init__(self) -> None:
        self.date = str(datetime.datetime.now()).split()[0]
    
    def _get_question_payload(self, question_nro: int) -> dict:
        '''
        Returns the payload for a single question and answer request.
        '''

        qa_payload = json.loads(GET_QUESTION_PAYLOAD)

        qa_payload['nro']=question_nro
        qa_payload['pointInTime']=self.date

        return qa_payload
    
    def _get_question_acts_payload(self, question_nro: int) -> dict:
        '''
        Return the payload for a single question related acts.
        '''

        qa_relationship_payload = json.loads(GET_QUESTION_ACTS_RELATIONSHIP_PAYLOAD)

        qa_relationship_payload['nro']=question_nro
        qa_relationship_payload['pointInTime']=self.date

        return qa_relationship_payload
    
    def _get_question_keywords_payload(self, question_id: int)-> dict:
        '''
        Returns the payload for a single question related keywords.
        '''

        keywords_payload = json.loads(GET_QUESTION_KEYWORDS_PAYLOAD)

        keywords_payload['id']=question_id
        keywords_payload['pointInTime']=self.date

        return keywords_payload
    
    def _get_question_search_payload(self,start_from:int=0,batch_size:int=25, domains: list[dict] = None) -> dict:
        '''
        Returns the payload for a search request for questions and answers within the given domains.
        '''
        
        domains_transformed = []
        if domains is not None:
            for dict in domains:
                for k,v in dict.items():
                    domains_transformed.append({"label": k, "conceptId":v})

        question_search_payload = json.loads(GET_QUESTION_SEARCH_PAYLOAD)

        question_search_payload['startFrom']=start_from
        question_search_payload['pointInTime']=self.date
        question_search_payload['domains']=domains_transformed
        question_search_payload['hitsPp']=batch_size

        return question_search_payload