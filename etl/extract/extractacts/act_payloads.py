import os
import json
import datetime

from dotenv import load_dotenv
load_dotenv()

GET_ACT_KEYWORDS_PAYLOAD = os.getenv('GET_ACT_KEYWORDS_PAYLOAD')

class ActPayloads():
    def __init__(self):
        self.date = str(datetime.datetime.now()).split()[0] 

    def get_act_keywords_payload(self, act_nro: int):
        '''
        Payload for keyword extraction for any legal act based on its id
        '''
        payload = json.loads(GET_ACT_KEYWORDS_PAYLOAD)
        payload['id'] = act_nro
        payload['pointInTime'] = self.date

        return payload