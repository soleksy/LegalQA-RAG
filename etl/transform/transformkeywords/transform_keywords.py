import os

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
            
    def transform_not_indexed_keywords(self) -> None:
        not_indexed = self._find_not_indexed_keywords()
        act_nros = self._get_act_nros()
        for keyword in not_indexed:
            self._transform_keyword(keyword, act_nros)

        self._find_missing_references()
        

        
        
        

