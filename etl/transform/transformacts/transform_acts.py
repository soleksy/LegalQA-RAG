import os
import tqdm
import json
import roman
import logging
import tiktoken

from langchain.text_splitter import CharacterTextSplitter
from transformers import GPT2TokenizerFast

from models.datamodels.act_vector import ActVector

from etl.common.keywordindex.transformed_keyword_index import TransformedKeywordIndex
from etl.common.questionindex.transformed_question_index import TransformedQuestionIndex
from etl.common.actindex.tree_act_index import TreeActIndex
from etl.common.actindex.leaf_node_act_index import LeafNodeActIndex



gpt2_tokenizer = GPT2TokenizerFast.from_pretrained("gpt2")
get_tokens = lambda text: len(gpt2_tokenizer.tokenize(text))


encoding = tiktoken.get_encoding('cl100k_base')
get_openai_tokens = lambda text: len(encoding.encode(text))



class TransformActs():
    def __init__(self):
        self.transformed_keyword_index = TransformedKeywordIndex()
        self.transformed_question_index = TransformedQuestionIndex()

        self.raw_acts_index = TreeActIndex()
        self.transformed_acts_index = LeafNodeActIndex()


        

    def _get_raw_index_acts(self) -> list[int]:

        raw_index_file_name = self.raw_acts_index._get_filename_index()
        raw_index_file_path = self.raw_acts_index.tree_acts_index_path + raw_index_file_name

        if os.path.exists(raw_index_file_path):
            return self.raw_acts_index._read_json_file(raw_index_file_path)
        else:
            return []

    def _get_transformed_index_acts(self) -> list[int]:

        transformed_index_file_name = self.transformed_acts_index._get_filename_index()
        transformed_index_file_path = self.transformed_acts_index.leaf_node_acts_index_path + transformed_index_file_name

        if os.path.exists(transformed_index_file_path):
                return self.transformed_acts_index._read_json_file(transformed_index_file_path)
        else:
            return []

    def _find_not_indexed_in_questions_acts(self) -> list[int]:
        '''
        Find acts that are not indexed in the questions index
        '''
        
        transformed_questions_file_name = self.transformed_question_index._get_filename_data()
        transformed_questions_data_path = self.transformed_question_index.transformed_questions_data_path + transformed_questions_file_name

        act_nros = set()
        if os.path.exists(transformed_questions_data_path):
            with open(transformed_questions_data_path, 'r') as f:
                transformed_questions = json.load(f)

            for question in transformed_questions['questions']:
                for relatedAct in transformed_questions['questions'][question]['relatedActs']:
                    if relatedAct['nro'] not in act_nros:
                        act_nros.add(relatedAct['nro'])
        
        return list(act_nros)
            
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
    
    def _add_keyword_to_elements(self, elements: dict, current_key: str, keyword: dict, processed_elements: set) -> None:
        '''
        Recursively add the keyword to the current_key node and its children, avoiding infinite recursion
        by using a set to track processed elements.
        '''
        # If current element is already processed, return to avoid infinite recursion
        if current_key in processed_elements:
            return

        # Mark the current element as processed
        processed_elements.add(current_key)

        # Add the keyword to the current element if it's not already there
        if keyword not in elements[current_key]['keywords']:
            elements[current_key]['keywords'].append(keyword)

        # Process children
        children = elements[current_key].get('children', [])
        for child in children:
            self._add_keyword_to_elements(elements, child, keyword, processed_elements)


    
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
                        self._add_keyword_to_elements(document['elements'], element, keyword, set())
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
                                    self._add_keyword_to_elements(document['elements'], key, keyword, set())
                                if key == right:
                                    break
                        else:
                            self._add_keyword_to_elements(document['elements'], unit['id'], keyword, set())
        return document

    def _chunk_text(self, text: str, num_chunks: int)-> list[str]:
        chunk_size = (get_tokens(text) // num_chunks) + 1
        text_splitter = CharacterTextSplitter.from_huggingface_tokenizer(
                                tokenizer=gpt2_tokenizer,
                                chunk_size=chunk_size,
                                chunk_overlap=0,
                                separator = ' ',
                                )
        
        return text_splitter.split_text(text)


    def _create_vector(self, act_nro: int, parent_id: str, chunk_id: int, total_chunks: int, parent_tokens: int, text_tokens: int, node_ids: list[str], text: str, keywords: list[str])-> dict:
        return {
            'act_nro': act_nro,
            'parent_id': parent_id,
            'chunk_id': chunk_id,
            'total_chunks': total_chunks,
            'parent_tokens': parent_tokens,
            'text_tokens': text_tokens,
            'node_ids': node_ids,
            'text': text,
            'keywords': keywords
        }
    
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
            keywords = []
            for node in subtree:
                node_ids.append(node['id'])
                for keyword in node['keywords']:
                    if keyword not in keywords:
                        keywords.append(keyword)

            if len(subtree) == 1:
                if get_tokens(subtree[0]['text']) < 512:
                    vectors.append(
                        self._create_vector(document['nro'], root_id, None, None, 0, get_openai_tokens(subtree[0]['text']), node_ids, subtree[0]['text'], keywords)
                    )
                else:
                    num_chunks = (get_tokens(subtree[0]['text']) // 512) + 1
                    texts = self._chunk_text(subtree[0]['text'], num_chunks)

                    for text in texts:
                        vectors.append(
                            self._create_vector(document['nro'], root_id, texts.index(text) + 1, len(texts), 0, get_openai_tokens(text), node_ids, text, keywords))
            else:
                concatenated_text  = ' '.join(node['text'] for node in subtree)
                total_tokens = get_tokens(concatenated_text)

                if total_tokens > 512:
                    num_chunks = (total_tokens // 512) + 1

                    #Chunking based on gpt-2 tokens
                    texts = self._chunk_text(concatenated_text, num_chunks)

                    for text in texts:
                        vectors.append(
                            self._create_vector(document['nro'], root_id, texts.index(text) + 1, len(texts), 0, get_openai_tokens(text), node_ids, text, keywords))
                else:
                    parentless_text = ' '.join(node['text'] for node in subtree[1:])
                    parent_tokens = get_openai_tokens(subtree[0]['text'])
                    vectors.append(
                        self._create_vector(document['nro'], root_id, None, None, parent_tokens, get_openai_tokens(parentless_text), node_ids, concatenated_text, keywords))
        return vectors , _set
    

    def _transform_act(self, document: dict)-> dict:
        transformed_document = {}

        transformed_document['nro'] = document['nro']
        transformed_document['title'] = document['title']
        transformed_document['actLawType'] = document['actLawType']

        url = document['citeLink']
        last_slash_pos = url.rfind('/')
        new_url = url[:last_slash_pos] + '?unitId='

        transformed_document['citeLink'] = new_url
        transformed_document['elements'] = {}
        transformed_document['reconstruct'] = {}

        _set = set()

        for element in document['elements']:
            if element not in _set:
                if document['elements'][element]['children'] == []:
                    root_id = self._extract_substrings(element)[0]
                    if root_id not in document['elements']:

                        root_id = self._fix_element(root_id)

                        if root_id not in document['elements']:
                           logging.error(f'Root id: {root_id} not found in {document["nro"]}')

                    vectors, _set = self._get_subtrees(root_id, document , _set)
                    transformed_document['elements'][root_id] = vectors

                    if len(vectors) == 1:

                        transformed_document['reconstruct'][root_id] = {
                            'cite_id': root_id,
                            'text': document['elements'][root_id]['text']
                            }
                        
                        vectors[0]['reconstruct_id'] = root_id
                        
                    elif len(vectors) > 1 and len(vectors[0]['node_ids']) == 1:

                        for vector in vectors:
                            transformed_document['reconstruct'][f'{root_id}_{vector["chunk_id"]}'] = {
                                'cite_id': root_id,
                                'text': vector['text'],
                            }

                            vector['reconstruct_id'] = f'{root_id}_{vector["chunk_id"]}'
                            
                    else:
                        transformed_document['reconstruct'][root_id] = {
                            'cite_id': root_id,
                            'text': document['elements'][root_id]['text']
                        }
                        
                        for vector in vectors:
                            if vector['chunk_id'] is None:
                                next_node = vector['node_ids'][1]

                                transformed_document['reconstruct'][next_node] = {
                                    'cite_id': next_node,
                                    'text': ' '.join(document['elements'][node]['text'] for node in vector['node_ids'][1:])
                                }

                                vector['reconstruct_id'] = next_node
                                
                            else:
                                next_node = vector['node_ids'][1]
                                _vector_text = vector['text']

                                transformed_document['reconstruct'][f'{next_node}_{vector["chunk_id"]}'] = {
                                    'cite_id': next_node,
                                    'text': _vector_text
                                }

                                vector['reconstruct_id'] = f'{next_node}_{vector["chunk_id"]}'

        for element in transformed_document['elements']:
            for i, vector in enumerate(transformed_document['elements'][element]):
                transformed_document['elements'][element][i] = ActVector(**vector).model_dump()


        return transformed_document
    
    def transform_acts(self)-> None:
        not_indexed_acts = self._find_not_indexed_in_questions_acts()

        for act_nro in tqdm.tqdm(not_indexed_acts):
            raw_act_file_name = self.raw_acts_index._get_filename_data(act_nro)
            raw_act_file_path = self.raw_acts_index.tree_acts_data_path + raw_act_file_name
            
            if os.path.exists(raw_act_file_path):
                raw_act = self.raw_acts_index._read_json_file(raw_act_file_path)
                keyworded_act = self._pass_keywords_onto_subtrees(raw_act)
                transformed_act = self._transform_act(keyworded_act)

                self.transformed_acts_index._update_act_data(act_nro, transformed_act)
                self.transformed_acts_index._update_act_index([act_nro])
                