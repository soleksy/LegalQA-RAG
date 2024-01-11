from bs4 import BeautifulSoup, NavigableString


class QuestionParser():
    def __init__(self) -> None:
        pass

    def parse_question_keyword_data(self, keywords_data: dict) -> dict:

        keywords_list = []

        if 'keywords' in keywords_data.keys():
            for keyword in keywords_data['keywords']:
                keywords_list.append({
                    'label': keyword['label'],
                    'conceptId': keyword['conceptId'],
                    'instanceOfType': keyword['instanceOfType']
                })

        return keywords_list

    def parse_question_act_data(self, acts_data: dict) -> dict:

        related_acts_list = []

        if 'documentList' in acts_data.keys():
            for document in acts_data['documentList']:
                
                nro = document['nro']
                document_title = document['title']
                law_type = document['lawType']
                validity = document['validity']
                
                relation_list = []
                for relation in document['relationData']['units']:
                    relation_list.append({
                        'nro': relation['nro'],
                        'id': relation['id'],
                        'name': relation['name'],
                    })

                if relation_list == []:
                    relation_list = None
                        
                related_acts_list.append({
                    'nro': nro,
                    'title': document_title,
                    'lawType': law_type,
                    'validity': validity,
                    'relationData': relation_list
                })

        return related_acts_list

    def parse_question_data(self, question_data: dict, act_data: dict, keyword_data: dict) -> dict:

        if question_data.get('questionContent') is not None:
            html = question_data['questionContent'] + question_data['answerContent']
        else:
            print(f"Could not find question content for qa with id: {question_data['nro']}")
            return None
        nro = question_data['nro']
        soup = BeautifulSoup(html, 'html.parser')

        for a in soup.find_all('a'):
            a.insert_before(NavigableString(' '))

        question = soup.find(
            'div', {
                'class': 'qa_q'}).get_text(
            strip=True, separator=' ') if soup.find(
                'div', {
                    'class': 'qa_q'}) else ""
        
        justification = soup.find('div',
                            {'class': 'qa_a-just'}).get_text(strip=True,
                                                             separator=' ') if soup.find('div',
                                                                                         {'class': 'qa_a-just'}) else ""
        answer = soup.find('div',
                         {'class': 'qa_a-cont'}).get_text(strip=True,
                                                          separator=' ') if soup.find('div',
                                                                                      {'class': 'qa_a-cont'}) else ""
        
        act_list = self.parse_question_act_data(act_data)
        keyword_list = self.parse_question_keyword_data(keyword_data)

        output_dict = {}
        output_dict['nro'] = nro

        if question_data['title'] != "":
            output_dict['title'] = question_data['title']
        else:
            output_dict['title'] = None

        if question != "":
            output_dict['question'] = question
        else:
            output_dict['question'] = None

        if answer != "":
            output_dict['answer'] = answer
        else:
            output_dict['answer'] = None

        if justification != "":
            output_dict['justification'] = justification
        else:
            output_dict['justification'] = None
        
        if act_list != []:
            output_dict['relatedActs'] = act_list
        else:
            output_dict['relatedActs'] = None

        if keyword_list != []:
            output_dict['keywords'] = keyword_list
        else:
            output_dict['keywords'] = None

        return output_dict