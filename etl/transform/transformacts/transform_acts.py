import os
import roman
import logging
import tiktoken

from langchain.text_splitter import CharacterTextSplitter
from transformers import GPT2TokenizerFast

from etl.common.keywordindex.transformed_keyword_index import TransformedKeywordIndex
from etl.common.actindex.tree_act_index import TreeActIndex
from etl.common.actindex.leaf_node_act_index import LeafNodeActIndex


gpt2_tokenizer = GPT2TokenizerFast.from_pretrained("gpt2")
get_tokens = lambda text: len(gpt2_tokenizer.tokenize(text))


encoding = tiktoken.get_encoding('cl100k_base')
get_openai_tokens = lambda text: len(encoding.encode(text))



class TransformActs():
    def __init__(self):
        self.transformed_keyword_index = TransformedKeywordIndex()

        self.raw_acts_index = TreeActIndex()
        self.transformed_acts_index = LeafNodeActIndex()
        

    def _get_raw_index_acts(self) -> list[int]:

        raw_index_file_name = self.raw_acts_index._get_filename_index()
        raw_index_file_path = self.raw_acts_index.tree_acts_index_path + raw_index_file_name

        if os.path.exists(raw_index_file_path):
            with open(raw_index_file_path, 'r') as f:
                return self.raw_acts_index._read_json_file(f)
        else:
            return []

    def _get_transformed_index_acts(self) -> list[int]:

        transformed_index_file_name = self.transformed_acts_index._get_filename_index()
        transformed_index_file_path = self.transformed_acts_index.leaf_node_acts_index_path + transformed_index_file_name

        if os.path.exists(transformed_index_file_path):
            with open(transformed_index_file_path, 'r') as f:
                return self.transformed_acts_index._read_json_file(f)
        else:
            return []

    def _find_not_indexed_acts(self) -> list[int]:
        raw_index_acts = self._get_raw_index_acts()
        transformed_index_acts = self._get_transformed_index_acts()

        if transformed_index_acts == []:
            return raw_index_acts
        else:
            transformed_index_acts_set = set(transformed_index_acts)
            raw_index_acts_set = set(raw_index_acts)

            not_indexed = list(raw_index_acts_set.difference(transformed_index_acts_set))

            if len(not_indexed) > 0:
                return not_indexed
            else:
                return []

    def _extract_substrings(self, element: str)-> list[str]:
        '''
        Given an element, extract all previous node elements in the tree
        '''
        stack = []
        substrings = []
        temp = ""

        for i, char in enumerate(element):
            if char == '(':
                stack.append(i)
            elif char == ')':
                if stack:
                    stack.pop()
                    if not stack:
                        substrings.append(element[:i + 1])
                else:
                    logging.error(f"Unmatched closing parenthesis for {element}")
            temp += char

        if stack:
            logging.error(f"Unmatched opening parenthesis for {element}")
        return substrings
    
    def _fix_element(self, element: str) -> str:
        '''
        Given an element which is not defined in the elements dict, fix it
        '''
        split = element.split('(')
        arabic = split[1][0]
        element = split[0] + '(' + roman.toRoman(int(arabic)) + split[1][1:]

        return element
