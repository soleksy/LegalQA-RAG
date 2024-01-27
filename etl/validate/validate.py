import os
import json
import tqdm
import logging

from etl.common.actindex.tree_act_index import TreeActIndex
from etl.common.questionindex.transformed_question_index import TransformedQuestionIndex
from etl.common.keywordindex.raw_keyword_index import RawKeywordIndex
from etl.common.keywordindex.transformed_keyword_index import TransformedKeywordIndex


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s' , force=True)


class Validate():
    def __init__(self):
        self.tree_acts_index = TreeActIndex()
        self.transformed_question_index = TransformedQuestionIndex()
        self.raw_keyword_index = RawKeywordIndex()
        self.transformed_keyword_index = TransformedKeywordIndex()
    
    def validate_dataset(self):
        questions_file_name = self.transformed_question_index._get_filename_data()
        questions_file_path = self.transformed_question_index.transformed_questions_data_path + questions_file_name
        
        with open(questions_file_path, 'r') as f:
            questions_file_data = json.load(f)

        questions_data = questions_file_data['questions']

        questions_act_nros = set()
        questions_keyword_ids = set()

        for question in questions_data:
            for relatedAct in questions_data[question]['relatedActs']:
                questions_act_nros.add(relatedAct['nro'])
            for keyword in questions_data[question]['keywords']:
                questions_keyword_ids.add((keyword['conceptId'], keyword['instanceOfType']))
        
        logging.info(f'Number of unique acts in questions: {len(questions_act_nros)}')
        logging.info(f'Number of unique keywords in questions: {len(questions_keyword_ids)}')

        acts_folder_path = self.tree_acts_index.tree_acts_data_path

        acts_act_nros = set()

        for file in os.listdir(acts_folder_path):
            if file.endswith('.json'):
                with open(acts_folder_path+file, 'r') as f:
                    act_data = json.load(f)
                acts_act_nros.add(act_data['nro'])

        logging.info(f'Number of unique acts in acts folder: {len(acts_act_nros)}')

        keywords_folder_path = self.raw_keyword_index.raw_keyword_data_path

        keywords_keyword_ids = set()

        for file in os.listdir(keywords_folder_path):
            if file.endswith('.json'):
                file_chunks = file.split('_')
                keyword_concept_id = file_chunks[0]
                keyword_instance_of_type = file_chunks[1].split('.')[0].replace('(' , '').replace(')', '')
                keywords_keyword_ids.add((int(keyword_concept_id), int(keyword_instance_of_type)))
            
        logging.info(f'Number of unique keywords in keywords folder: {len(keywords_keyword_ids)}')

        keywords_index_folder_path = self.raw_keyword_index.raw_keyword_index_path
        keywords_index_file_path = keywords_index_folder_path + self.raw_keyword_index._get_filename_index()

        keywords_index_keyword_ids = set()

        with open(keywords_index_file_path, 'r') as f:
            keywords_index_data = json.load(f)

            for keyword in keywords_index_data:
                keywords_index_keyword_ids.add((keyword['conceptId'], keyword['instanceOfType']))
        
        logging.info(f'Number of unique keywords in keywords index: {len(keywords_index_keyword_ids)}')

    def validate_act_keyword_relationship(self):
        acts_folder_path = self.tree_acts_index.tree_acts_data_path

        for file in os.listdir(acts_folder_path):
            if file.endswith('.json'):
                with open(acts_folder_path+file, 'r') as f:
                    act_data = json.load(f)
                
                act_keywords = act_data['keywords']
                act_nro = act_data['nro']

                for keyword in act_keywords:
                    folder_path = self.raw_keyword_index.raw_keyword_data_path
                    file_name_keyword =self.raw_keyword_index._get_filename_data(keyword)
                    if os.path.exists(folder_path+file_name_keyword):
                        with open(folder_path+file_name_keyword, 'r') as f:
                            keyword_data = json.load(f)
                        if keyword_data == []:
                            continue
                        if keyword_data.get(str(act_nro)) == None:
                            logging.info(f'Act {act_nro} not found in keyword {keyword["conceptId"]}')
    
    def validate_act_keyword_transformed(self):
        acts_folder_path = self.tree_acts_index.tree_acts_data_path

        for file in os.listdir(acts_folder_path):
            if file.endswith('.json'):
                with open(acts_folder_path+file, 'r') as f:
                    act_data = json.load(f)
                
                act_keywords = act_data['keywords']
                act_nro = act_data['nro']

                for keyword in act_keywords:
                    folder_path = self.transformed_keyword_index.transformed_keyword_data_path
                    file_name_keyword =self.transformed_keyword_index._get_filename_data(keyword)
                    if os.path.exists(folder_path+file_name_keyword):
                        with open(folder_path+file_name_keyword, 'r') as f:
                            keyword_data = json.load(f)
                        if keyword_data == []:
                            continue
                        if keyword_data.get(str(act_nro)) == None:
                            logging.info(f'Act {act_nro} not found in keyword {keyword["conceptId"]}')
        
        raw_keyword_dir = self.raw_keyword_index.raw_keyword_data_path
        transformed_keyword_dir = self.transformed_keyword_index.transformed_keyword_data_path

        raw_files = set()
        transformed_files = set()

        for file in os.listdir(raw_keyword_dir):
            if file.endswith('.json'):
                raw_files.add(file)
        
        for file in os.listdir(transformed_keyword_dir):
            if file.endswith('.json'):
                transformed_files.add(file)
        
        if raw_files.difference(transformed_files) != set():
            logging.info('Number of files in raw and transformed keyword data folder is not the same')

    def validate_keyword_units(self):
        transformed_keyword_index = TransformedKeywordIndex()
        folder_path = transformed_keyword_index.transformed_keyword_data_path

        raw_act_index = TreeActIndex()
        act_data_path = raw_act_index.tree_acts_data_path

        correct = 0
        incorrect = 0

        for file in tqdm.tqdm(os.listdir(folder_path)):
            if file.endswith('.json'):
                with open(folder_path+file, 'r') as f:
                    keyword_data = json.load(f)
                
                for act_nro in keyword_data:
                    act_filename = raw_act_index._get_filename_data(act_nro)
                    if os.path.exists(act_data_path+act_filename):
                        with open(act_data_path+act_filename, 'r') as f:
                            act_data = json.load(f)

                    if keyword_data[act_nro]['relationData'] == {}:
                        continue
                    else:
                        for unit in keyword_data[act_nro]['relationData']['units']:
                            if '-' in unit['id']:
                                left = unit['id'].split('-')[0]
                                right = unit['id'].split('-')[1]
                                if left not in act_data['elements'] or right not in act_data['elements']:
                                    incorrect += 1
                                else:
                                    correct += 1
                            else:
                                if unit['id'] not in act_data['elements']:
                                    incorrect += 1
                                else:
                                    correct += 1
                                    
        logging.info(f'correct vs incorrect: {correct} vs {incorrect}')
    
    
    def _extract_substrings(self, pattern: str)-> list[str]:
        stack = []
        substrings = []
        temp = ""

        for i, char in enumerate(pattern):
            if char == '(':
                stack.append(i)
            elif char == ')':
                if stack:
                    start = stack.pop()
                    if not stack:
                        substrings.append(pattern[:i + 1])
                else:
                    raise ValueError("Unmatched closing parenthesis")
            temp += char

        if stack:
            raise ValueError("Unmatched opening parenthesis")
        return substrings


    def _find_errors_in_element_ids(self):
        raw_act_index = TreeActIndex()
        act_data_path = raw_act_index.tree_acts_data_path

        for file in tqdm.tqdm(os.listdir(act_data_path)):
            if file.endswith('.json'):
                with open(act_data_path+file, 'r') as f:
                    act_data = json.load(f)
                    elements = act_data['elements']

                if elements == {}:
                    continue
                else:
                    for element in elements:
                        if ' ' in element:
                            print(f'Error in act {act_data["nro"], {element}}')
                        
                        for child in elements[element]['children']:
                            if ' ' in child:
                                print(f'Error in act {act_data["nro"], {child}}')
                            if child not in elements:
                                print(f'Error in act {act_data["nro"], {child}}')
    
    def check_no_backward_references(self, document: dict, root_id: str)-> bool:
        def dfs(node_id, parent_id=None):

            node = document.get(node_id, {})
            
            for child_id in node.get('children', []):
                if parent_id and parent_id in document.get(child_id, {}).get('children', []):
                    return False
                
                if not dfs(child_id, node_id):
                    return False
                    
            return True

        return dfs(root_id)

    def _validate_act_tree_structure(self):
        raw_act_index = TreeActIndex()
        act_data_path = raw_act_index.tree_acts_data_path
        for file in tqdm.tqdm(os.listdir(act_data_path)):
            if file.endswith('.json'):
                with open(act_data_path+file, 'r') as f:
                    act_data = json.load(f)
                    elements = act_data['elements']

                if elements == {}:
                    continue
                else:
                    for element in elements:
                        if act_data['elements'][element]['children'] == []:
                            parent = self._extract_substrings(element)[0]
                            isTraversable = self.check_no_backward_references(act_data['elements'], parent)

                            if isTraversable:
                                continue
                            else:
                                print(f'Error in act {act_data["nro"], {element}}')