import os
import re
import tqdm
import json
import logging

from etl.common.keywordindex.raw_keyword_index import RawKeywordIndex
from etl.common.keywordindex.transformed_keyword_index import TransformedKeywordIndex
from etl.common.actindex.tree_act_index import TreeActIndex


class TransformKeywords():
    def __init__(self) -> None:
        self.raw_index = RawKeywordIndex()
        self.transformed_index = TransformedKeywordIndex()
        self.tree_index = TreeActIndex()
    
    def _get_raw_index_keywords(self) -> list[dict]:
        raw_index_file_name = self.raw_index._get_filename_index()
        raw_index_file_path = self.raw_index.raw_keyword_index_path+raw_index_file_name

        if os.path.exists(raw_index_file_path):
            return self.raw_index._read_json_file(raw_index_file_path)
        else:
            return []

    def _get_transformed_index_keywords(self) -> list[dict]:
        transformed_index_file_name = self.transformed_index._get_filename_index()
        transformed_index_file_path = self.transformed_index.transformed_keyword_index_path+transformed_index_file_name

        if os.path.exists(transformed_index_file_path):
            return self.transformed_index._read_json_file(transformed_index_file_path)
        else:
            return []
        
    def _find_not_indexed_keywords(self) -> list[dict]:
        
        transformed_index = self._get_transformed_index_keywords()
        raw_index = self._get_raw_index_keywords()
        if transformed_index == []:
            return [{'conceptId': keyword['conceptId'], 'instanceOfType': keyword['instanceOfType']} for keyword in raw_index]
        else:
            transformed_index_set = set([(keyword['conceptId'], keyword['instanceOfType']) for keyword in transformed_index])
            raw_index_set = set([(keyword['conceptId'], keyword['instanceOfType']) for keyword in raw_index])

            not_indexed = list(raw_index_set.difference(transformed_index_set))
            not_indexed = [{'conceptId': keyword[0], 'instanceOfType': keyword[1]} for keyword in not_indexed]

            if len(not_indexed) > 0:
                return not_indexed
            else:
                return []

    def _get_act_nros(self) -> list[int]:
        return self.tree_index._get_act_nros()
    
    def _transform_keyword(self, keyword: dict , act_nros: list[int]) -> None:
        raw_keyword_data_file_name = self.raw_index._get_filename_data(keyword)
        raw_keyword_data_file_path = self.raw_index.raw_keyword_data_path+raw_keyword_data_file_name

        transformed_keyword_data = {}

        if os.path.exists(raw_keyword_data_file_path):
            raw_keyword_data = self.raw_index._read_json_file(raw_keyword_data_file_path)

            for keyword_act_nro in raw_keyword_data:
                if int(keyword_act_nro) in act_nros:
                    transformed_keyword_data[keyword_act_nro] = raw_keyword_data[keyword_act_nro]
        
        transformed_keyword_data_file_name = self.transformed_index._get_filename_data(keyword)
        transformed_keyword_data_file_path = self.transformed_index.transformed_keyword_data_path+transformed_keyword_data_file_name

        if os.path.exists(transformed_keyword_data_file_path):
            return None
        else:
            self.transformed_index._write_json_file(transformed_keyword_data_file_path, transformed_keyword_data)
            self.transformed_index._update_keyword_index([keyword])
        
    def _find_missing_references(self) -> None:
        tree_acts_path = self.tree_index.tree_acts_data_path

        for file in os.listdir(tree_acts_path):
            if file.endswith('.json'):
                act_data = self.tree_index._read_json_file(tree_acts_path+file)
                act_nro = act_data['nro']
                act_keywords = act_data['keywords']

                reference_dict = {
                    'title': act_data['title'],
                    'nro': act_nro,
                    'lawType': act_data['actLawType'],
                    'validity': 'ACTUAL',
                    'relationData': {}
                }

                for keyword in act_keywords:
                    transformed_keyword_data_file_name = self.transformed_index._get_filename_data(keyword)
                    transformed_keyword_data_file_path = self.transformed_index.transformed_keyword_data_path+transformed_keyword_data_file_name

                    if os.path.exists(transformed_keyword_data_file_path):
                        keyword_data = self.transformed_index._read_json_file(transformed_keyword_data_file_path)
                        if keyword_data.get(str(act_nro)) == None:
                            keyword_data[str(act_nro)] = reference_dict
                            self.transformed_index._write_json_file(transformed_keyword_data_file_path, keyword_data)
    
    def _roman_to_arabic(self, roman):

        roman_numerals = {'I': 1, 'V': 5, 'X': 10, 'L': 50, 'C': 100, 'D': 500, 'M': 1000}
        prev_value = total = 0

        for char in reversed(roman.upper()):

            value = roman_numerals[char]

            if value < prev_value:
                total -= value
            else:
                total += value

            prev_value = value
        return total

    def _replace_nested_uppercase_roman_with_arabic(self, text):

        roman_pattern = r'\(([MDCLXVI]+)(\([^)]*\))?\)'
        
        def repl(match):
            roman_numeral = match.group(1)  
            nested_content = match.group(2) if match.group(2) else ''  
            arabic_numeral = self._roman_to_arabic(roman_numeral)  
            return f'({arabic_numeral}{nested_content})'
        
        return re.sub(roman_pattern, repl, text)

    def _correct_bracket_errors(self,text):

        pattern = r'(\d+)([a-zA-Z])|([a-zA-Z])(\d+)'
        
        def replacement(match):
            if match.group(2): 
                return f'{match.group(1)}({match.group(2)})'
            else: 
                return f'{match.group(3)}({match.group(4)})'

        corrected_text = re.sub(pattern, replacement, text)
        
        return corrected_text
    
    def _fix_broken_references(self) -> None:
        transformed_keyword_index = TransformedKeywordIndex()
        folder_path = transformed_keyword_index.transformed_keyword_data_path

        raw_act_index = TreeActIndex()
        act_data_path = raw_act_index.tree_acts_data_path

        for file in tqdm.tqdm(os.listdir(folder_path)):
            if file.endswith('.json'):
                with open(folder_path+file, 'r') as f:
                    keyword_data = json.load(f)
                
                cleaned_keyword_data = {}
                for act_nro in keyword_data:
                    act_filename = raw_act_index._get_filename_data(act_nro)
                    if os.path.exists(act_data_path+act_filename):
                        with open(act_data_path+act_filename, 'r') as f:
                            act_data = json.load(f)

                    if keyword_data[act_nro]['relationData'] == {}:
                        cleaned_keyword_data[act_nro] = keyword_data[act_nro]
                    else:
                        clean_units = []
                        for unit in keyword_data[act_nro]['relationData']['units']:
                            if '-' in unit['id']:
                                left = unit['id'].split('-')[0]
                                right = unit['id'].split('-')[1]

                                if left not in act_data['elements'] or right not in act_data['elements']:
                                    left = self._replace_nested_uppercase_roman_with_arabic(left)
                                    left = left.lower()
                                    left = self._correct_bracket_errors(left)
                                    left.replace('@' , '')

                                    
                                    right = self._replace_nested_uppercase_roman_with_arabic(right)
                                    right = right.lower()
                                    right = self._correct_bracket_errors(right)
                                    right.replace('@' , '')

                                    if left not in act_data['elements'] or right not in act_data['elements']:
                                        logging.info(f'Act {act_nro} does not contain element {left}')
                                        logging.info(f'In keyword {file}')
                                    else:
                                        clean_id = left + '-' + right
                                        clean_units.append({
                                            'nro': unit['nro'],
                                            'id': clean_id,
                                            'name': unit['name'],
                                            'stateName': unit['stateName']
                                        })
                                else:
                                    clean_units.append({
                                        'nro': unit['nro'],
                                        'id': unit['id'],
                                        'name': unit['name'],
                                        'stateName': unit['stateName']
                                    })
                            else:
                                if unit['id'] not in act_data['elements']:
                                    unit_id  = unit['id']
                                    unit_id = self._replace_nested_uppercase_roman_with_arabic(unit_id)
                                    unit_id = unit_id.lower()
                                    unit_id = self._correct_bracket_errors(unit_id)
                                    unit_id = unit_id.replace('@' , '')

                                    if unit_id not in act_data['elements']:
                                        logging.info(f'Act {act_nro} does not contain element {unit_id}')
                                        logging.info(f'In keyword {file}')
                                    else:
                                        clean_units.append({
                                            'nro': unit['nro'],
                                            'id': unit_id,
                                            'name': unit['name'],
                                            'stateName': unit['stateName']
                                        })
                                else:
                                    clean_units.append({
                                        'nro': unit['nro'],
                                        'id': unit['id'],
                                        'name': unit['name'],
                                        'stateName': unit['stateName']
                                    })

                        if len(clean_units) > 0:
                            cleaned_keyword_data[act_nro] = keyword_data[act_nro]
                            cleaned_keyword_data[act_nro]['relationData']['units'] = clean_units
                            
                with open(folder_path+file, 'w') as f:
                    json.dump(cleaned_keyword_data, f, indent=4, ensure_ascii=False)


    def transform_not_indexed_keywords(self) -> None:
        not_indexed = self._find_not_indexed_keywords()
        act_nros = self._get_act_nros()
        for keyword in not_indexed:
            self._transform_keyword(keyword, act_nros)

        self._find_missing_references()
        self._fix_broken_references()
        

        
        
        

