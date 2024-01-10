import os
import json
import dotenv
import datetime


dotenv.load_dotenv()
GET_QUESTION_PAYLOAD=os.getenv('GET_QUESTION_PAYLOAD')
GET_QUESTION_ACTS_RELATIONSHIP_PAYLOAD=os.getenv('GET_QUESTION_ACTS_RELATIONSHIP_PAYLOAD')
GET_QUESTION_KEYWORDS_PAYLOAD=os.getenv('GET_QUESTION_KEYWORDS_PAYLOAD')

class QuestionPayloads():
    def __init__(self) -> None:
        self.date = str(datetime.datetime.now()).split()[0]
    
    def get_question_payload(self, question_nro: int) -> dict:
        '''
        Returns the payload for a single question and answer request.
        '''

        qa_payload = json.loads(GET_QUESTION_PAYLOAD)

        qa_payload['nro']=question_nro
        qa_payload['pointInTime']=self.date

        return qa_payload
    
    def get_question_acts_payload(self, question_nro: int) -> dict:
        '''
        Return the payload for a single question related acts.
        '''

        qa_relationship_payload = json.loads(GET_QUESTION_ACTS_RELATIONSHIP_PAYLOAD)

        qa_relationship_payload['nro']=question_nro
        qa_relationship_payload['pointInTime']=self.date

        return qa_relationship_payload
    
    def get_question_keywords_payload(self, question_id: int)-> dict:
        '''
        Returns the payload for a single question related keywords.
        '''

        keywords_payload = json.loads(GET_QUESTION_KEYWORDS_PAYLOAD)

        keywords_payload['id']=question_id
        keywords_payload['pointInTime']=self.date

        return keywords_payload