import os

from etl.common.actindex.act_index_base import ActIndexBase


class LeafNodeActIndex(ActIndexBase):
    def __init__(self):
        super().__init__()
    

    def _get_filename_index(self) -> str:
        return 'leaf_node_act_index.json'
    
    def _get_filename_data(self, act_nro: int) -> str:
        return str(act_nro) + '_leaf_node_act_data.json'
    
    def _find_missing_nros(self, nro_list: list[int]) -> list[int]:
        file_name = self._get_filename_index()
        file_path = self.tree_acts_index_path+file_name

        if os.path.exists(file_path):
            index = self._read_json_file(file_path)

            missing = list(set(nro_list).difference(set(index)))

            if len(missing) > 0:
                return missing
            else:
                return []
        else:
            return nro_list
    
    def _update_act_index(self, nro_list: list[int]) -> None:
        file_name = self._get_filename_index()
        file_path = self.leaf_node_acts_index_path+file_name

        if os.path.exists(file_path):
            index = self._read_json_file(file_path)
            updated_index = list(set(index + nro_list))
            self._write_json_file(file_path, updated_index)
        else:
            index = nro_list
            self._write_json_file(file_path, index)
    
    def _update_act_data(self, act_nro: int, act_data: dict) -> None:
        file_name = self._get_filename_data(act_nro)
        file_path = self.leaf_node_acts_data_path+file_name

        if os.path.exists(file_path):
            return None
        else:
            self._write_json_file(file_path, act_data)
    
    def _get_act_nros(self)-> list[int]:
        file_name = self._get_filename_index()
        file_path = self.leaf_node_acts_index_path+file_name

        if os.path.exists(file_path):
            return self._read_json_file(file_path)
        else:
            return []

    def _validate_act_index(self) -> bool:
        index_file_name = self._get_filename_index()
        index_file_path = self.leaf_node_acts_index_path+index_file_name

        indexed = self._read_json_file(index_file_path)

        for index in indexed:
            data_file_name = self._get_filename_data(index)
            data_file_path = self.leaf_node_acts_data_path+data_file_name

            if not os.path.exists(data_file_path):
                return False
            
        return True