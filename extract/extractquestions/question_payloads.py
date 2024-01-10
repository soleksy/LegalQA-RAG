import os
import json
import dotenv
import datetime


dotenv.load_dotenv()
GET_QUESTION_PAYLOAD=os.getenv('GET_QUESTION_PAYLOAD')

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