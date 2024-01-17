import logging

from bs4 import BeautifulSoup , NavigableString , Tag
from typing import Dict


from models.datamodels.tree_act import TreeAct , Element


class ActParser:
    def __init__(self) -> None:
        pass

    def unit_id_to_div_id(self,unit_id: str) -> str:
        return "\\\"" + unit_id + "\\\""

    def div_id_to_unit_id(self,div_id: str) -> str:
        return div_id.strip("\\\"")

    def find_divs_at_target_level(self, soup: BeautifulSoup, parent_id: str, target_ids: list[str]) -> list[str]:
        target_ids_set = set(target_ids)
        def search_divs(element) -> list[str]:
            if element is None:
                print(f"No div found with id: {parent_id}")
                return []
            result=[]
            if element.name == 'div':
                child_div_ids = [child.get('id') for child in element.find_all('div', recursive=False) if child.get('id')]
                if any(id in target_ids_set for id in child_div_ids):
                    for id in child_div_ids:
                        if id in target_ids_set:
                            result.append(id)
                    return child_div_ids
            
            for child in element.children:
                if isinstance(child, Tag):
                    result = search_divs(child)
                    if result:
                        return result
            return []
        if ' ' in parent_id:
            print(f"Parent id contains space: {parent_id}")
        parent_div = soup.find('div', id=parent_id)
        return search_divs(parent_div)

    def get_text_excluding_children(self,soup: BeautifulSoup, parent_id: str, exclude_ids: list[str]) -> str:
        exclude_ids_set = set(exclude_ids)
        result_text = ''

        parent_div = soup.find('div', id=parent_id)
        if not parent_div:
            return result_text

        def extract_text(element) -> None:
            nonlocal result_text
            for content in element.contents:
                if isinstance(content, NavigableString):
                    result_text += content.strip() + ' '
                elif content.name == 'div' and content.get('id') in exclude_ids_set:
                    continue
                elif hasattr(content, 'contents'):
                    extract_text(content)

        extract_text(parent_div)
        return result_text.strip()
    
    def parse_single_act(self, html_content, tree_act: TreeAct, data: dict):
        soup = BeautifulSoup(html_content, 'html.parser')
        if data.get('units') is None:
            logging.error(f"Could not find units for act with id: {tree_act.nro}")
            return None
        
        div_ids = [self.unit_id_to_div_id(unit['unitId']) for unit in data['units']]
        elements_dict: Dict[str, Element] = {}
        for id in div_ids:
            elements_dict[id] = Element(
            children=self.find_divs_at_target_level(soup, id, div_ids),
            parent=None,
            text="",
            keywords=[]
        )
            
        # Set parents and update text
        for id in div_ids:
            elements_dict[id].parent = None
            for child_id in div_ids:
                if id in elements_dict[child_id].children:
                    elements_dict[id].parent = child_id
                    break
            elements_dict[id].text = self.get_text_excluding_children(soup, id, elements_dict[id].children)

        # Clean and fix the data
        for id in div_ids:
            elements_dict[id].children = [self.div_id_to_unit_id(child) for child in elements_dict[id].children]
            if elements_dict[id].parent is not None:
                elements_dict[id].parent = self.div_id_to_unit_id(elements_dict[id].parent)
            elements_dict[id].text = elements_dict[id].text.replace('\\\"', '').replace('  ', ' ').replace('\u00A0', '')
        
        clean_dict = {}
        for key in elements_dict.keys():
            clean_dict[self.div_id_to_unit_id(key)] = elements_dict[key]

        if tree_act.title is not None:
            tree_act.title  = tree_act.title .replace('\\\"', '').replace('  ', ' ').replace('\u00A0', '')
        if tree_act.shortQuote  is not None:
            tree_act.shortQuote = tree_act.shortQuote.replace('\\\"', '').replace('  ', ' ').replace('\u00A0', '')
        
        tree_act.elements = clean_dict

        return tree_act