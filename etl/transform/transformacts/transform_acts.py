import os
import json
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
    
    def _add_keyword_to_elements(self, elements: dict[dict], current_key: str, keyword: dict) -> None:
        '''
        Recursively add the keyword to the current_key node and children of the current_key
        '''
        if keyword not in elements[current_key]['keywords']:
            elements[current_key]['keywords'].append(keyword)

        for child in elements[current_key].get('children', []):
            self._add_keyword_to_elements(elements, child, keyword)
    
    def _pass_keywords_onto_subtrees(self, document: dict)-> dict:

        folder_data_path = self.transformed_keyword_index.transformed_keyword_data_path
        act_nro = document['nro']

        for keyword in document['keywords']:
            file_name = self.transformed_keyword_index._get_filename_data(keyword)
            if os.path.exists(folder_data_path + file_name):
                with open(folder_data_path + file_name, 'r') as f:
                    keyword_data = json.load(f)

            if keyword_data.get(str(act_nro), None) is None:
                continue
            elif keyword_data.get(str(act_nro))['relationData'] == {}:
                for element in document['elements']:
                    self._add_keyword_to_elements(document['elements'], element, keyword)
            else:
                for unit in keyword_data[str(act_nro)]['relationData']['units']:
                    if '-' in unit['id']:

                        start_processing = False
                        
                        left = unit['id'].split('-')[0]
                        right = unit['id'].split('-')[1]

                        for key in document['elements']:
                            if key == left:
                                start_processing = True
                            if start_processing:
                                self._add_keyword_to_elements(document['elements'], key, keyword)
                            if key == right:
                                break
                    else:
                        self._add_keyword_to_elements(document['elements'], unit['id'], keyword)
        return document

    def _chunk_text(self, text: str, num_of_chunks: int)-> list[str]:
        chunk_size = (get_tokens(text) // num_of_chunks) + 1

        text_splitter = CharacterTextSplitter.from_huggingface_tokenizer(
                                tokenizer=gpt2_tokenizer,
                                chunk_size=chunk_size,
                                chunk_overlap=0,
                                separator = ' ',
                                )
        
        return text_splitter.split_text(text)

    def _get_subtrees(self, root_id: str, document: dict ,_set: set() = set())-> (list[dict],set()):
        '''
        Given a root_id of a document subtree, find all its nodes and return a list of vectors representing the subtrees
        '''

        elements = document['elements']
        _set.add(root_id)

        def traverse(node_id, _set: set() = _set):

            _set.add(node_id)
            node = elements.get(node_id, {})
            subtree = [{'id': node_id, 'text': node.get('text', '') , 'keywords': node.get('keywords', [])}]

            for child_id in node.get('children', []):
                subtree.extend(traverse(child_id))
            return subtree

        #Create subtrees
        root_node = elements.get(root_id, {})
        root_node_entry = [{'id': root_id, 'text': root_node.get('text', '') , 'keywords': root_node.get('keywords', [])}]
        subtrees = []
        children = root_node.get('children', [])

        if children == []:
            subtrees.append(root_node_entry)
        else:
            for child_id in root_node.get('children', []):
                subtree = root_node_entry + traverse(child_id , _set)
                subtrees.append(subtree)
        
        #Split subtrees into vectors of at most 512 tokens
        vectors = []

        for subtree in subtrees:
            total_tokens = 0
            node_ids = []
            concatenated_text = ''
            keywords = []
            for node in subtree:
                node_ids.append(node['id'])
                for keyword in node['keywords']:
                    if keyword not in keywords:
                        keywords.append(keyword)
            
            concatenated_text  = ' '.join(node['text'] for node in subtree)
            total_tokens = get_tokens(concatenated_text)

            parentless_text = ' '.join(node['text'] for node in subtree[1:])

            if total_tokens > 512:
                num_chunks = (total_tokens // 512) + 1

                #Chunking based on gpt-2 tokens
                texts = self._chunk_text(concatenated_text, num_chunks)
                parentless_texts = self._chunk_text(parentless_text, num_chunks)

                for text in texts:
                    vectors.append({
                        'act_nro': document['nro'],
                        'parent_id': root_id,
                        'chunk_id': texts.index(text) + 1,
                        'total_chunks': len(texts),
                        'parent_tokens': get_openai_tokens(elements[root_id]['text']),
                        'text_tokens': get_openai_tokens(parentless_texts[texts.index(text)]),
                        'node_ids': node_ids ,
                        'text': text,
                        'parentless_text': parentless_texts[texts.index(text)],
                        'keywords': keywords
                        })
            else:
                vectors.append({
                    'act_nro': document['nro'],
                    'parent_id': root_id,
                    'chunk_id': None,
                    'total_chunks': None,
                    'parent_tokens': get_openai_tokens(elements[root_id]['text']),
                    'openai_tokens': get_openai_tokens(parentless_text),
                    'node_ids': node_ids, 
                    'text': concatenated_text,
                    'keywords': keywords
                    })
                
        return vectors , _set