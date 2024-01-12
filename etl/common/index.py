import json


class Index():
    def __init__(self):
        with open('etl/common/config.json') as f:
            self.config = json.load(f)

        self.raw_questions_index_path = self.config['raw_questions_index']
        self.raw_questions_data_path = self.config['raw_questions_data']