import os

from etl.common.keywordindex.keyword_index_base import KeywordIndexBase


class RawKeywordIndex(KeywordIndexBase):
    def __init__(self) -> None:
        super().__init__()

    def _get_filename_index(self) -> str:
        return 'raw_keyword_index.json'
    
    def _get_filename_data(self, keyword: dict) -> str:
        conceptId = keyword['conceptId']
        instanceOfType = keyword['instanceOfType']
        return f'{conceptId}_{instanceOfType}_raw_keyword_data.json'
    
    def _find_missing_keywords(self, keyword_list: list[dict]) -> list[dict]:
        file_name = self._get_filename_index()
        file_path = self.raw_keyword_index_path+file_name

        keyword_set = set([(keyword['conceptId'], keyword['instanceOfType']) for keyword in keyword_list])

        if os.path.exists(file_path):
            index = self._read_json_file(file_path)
            index_set = set([(keyword['conceptId'], keyword['instanceOfType']) for keyword in index])

            missing = list(keyword_set.difference(index_set))
            
            if len(missing) > 0:
                return missing
            else:
                return []
        else:
            return list({'conceptId': keyword['conceptId'], 'instanceOfType': keyword['instanceOfType']} for keyword in keyword_list)
        
    def _update_keyword_index(self, keyword_list: list[dict]) -> None:
        file_name = self._get_filename_index()
        file_path = self.raw_keyword_index_path+file_name

        if os.path.exists(file_path):
            index = self._read_json_file(file_path)

            index_set = set([(keyword['conceptId'], keyword['instanceOfType']) for keyword in index])
            keyword_set = set([(keyword['conceptId'], keyword['instanceOfType']) for keyword in keyword_list])

            updated_index = list(set(index_set + keyword_set))

            to_write = list({'conceptId': keyword[0], 'instanceOfType': keyword[1]} for keyword in updated_index)
            self._write_json_file(file_path, to_write)
        else:
            index = keyword_list
            self._write_json_file(file_path, index)