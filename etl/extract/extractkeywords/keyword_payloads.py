import os
import json
import datetime

from dotenv import load_dotenv
load_dotenv()



GET_KEYWORD_PAYLOAD = os.getenv('GET_KEYWORD_PAYLOAD')


class KeywordPayloads():
    def __init__(self):
        self.date = str(datetime.datetime.now()).split()[0] 

    def _get_keyword_payload(self, keyword_id:int, ui_concept_id:int=-1, start_from:int= 0, batch_size:int=25) -> dict:
        '''
        Payload for keyword extraction based on keyword id and ui_concept_id
        '''
        payload = json.loads(GET_KEYWORD_PAYLOAD)
        payload['uiConceptId'] = keyword_id
        payload['uiInstanceOfType'] = str(ui_concept_id)
        payload['startFrom'] = start_from
        payload['pointInTime'] = self.date
        payload['hitsPp'] = batch_size

        return payload
