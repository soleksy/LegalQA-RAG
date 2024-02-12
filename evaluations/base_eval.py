import tqdm
import torch
import random
import logging

from sentence_transformers import SentenceTransformer
from typing import List


from qdrantdb.collections.qdrant_question_collection import QdrantQuestionCollection
from qdrantdb.collections.qdrant_act_collection import QdrantActCollection
from etl.common.questionindex.transformed_question_index import TransformedQuestionIndex
from models.datamodels.question import *
from models.datamodels.act_vector import *


logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)


async def get_model():
    model = SentenceTransformer("sdadas/mmlw-retrieval-roberta-large")
    model = model.to("cuda" if torch.cuda.is_available() else "cpu")
    return model

class BaseEval:
    def __init__(self) -> None:
        self.qdrant_question_collection: QdrantQuestionCollection = None
        self.qdrant_act_collection: QdrantActCollection = None
        self.model = None

        self.question_index: TransformedQuestionIndex = TransformedQuestionIndex()

    @classmethod
    async def create(cls) -> 'BaseEval':
        instance = cls()
        instance.qdrant_question_collection = await QdrantQuestionCollection.create()
        instance.qdrant_act_collection = await QdrantActCollection.create()
        instance.model = await get_model()
        return instance

    def get_questions_to_evaluate(self) -> List[Question]:
        folder = self.question_index.transformed_questions_data_path
        file = self.question_index._get_filename_data()

        data = self.question_index._read_json_file(folder+file)

        #Get only questions with available 
        with_citations = []
        is_approved = True
        for question in data['questions']:
            is_approved = True
            for related_act in data['questions'][question]['relatedActs']:
                if related_act['relationData'] == []:
                    is_approved = False
                    break
            if is_approved:
                with_citations.append(Question(**data['questions'][question]))

        return self.randomize_questions(with_citations)

    def randomize_questions(self, questions: list[Question]) -> List[Question]:
        ten_percent_count = len(questions) // 80 
        
        if ten_percent_count == 0 and questions:
            ten_percent_count = 1

        return random.sample(questions, ten_percent_count)
    
    async def evaluate_acts_only(self) -> None:
        questions: List[Question] = self.get_questions_to_evaluate()
        total_cite_id_hit_rate = 0
        act_vectors_returned = 10
        for question in tqdm.tqdm(questions):
            question_vector = self.model.encode('zapytanie: ' + question.title, convert_to_tensor=False, show_progress_bar=False)
            question_cite_ids = set([(relation_data.nro ,relation_data.id) for related_act in question.relatedActs for relation_data in related_act.relationData])
            question_acts_nros = set([related_act.nro for related_act in question.relatedActs])
            question_keywords = [keyword for keyword in question.keywords]

            acts = await self.qdrant_act_collection.search_acts_keyword_filtered(limit=act_vectors_returned,act_nros=list(question_acts_nros),keywords=question_keywords,vector=question_vector)

            results_cite_ids = set()

            for cite_vector in acts:
                cite_vector = ActVector(**cite_vector.payload)
                for node_id in cite_vector.node_ids:
                    results_cite_ids.add((cite_vector.act_nro, node_id))

            cite_id_intersection = question_cite_ids.intersection(results_cite_ids)
            total_cite_id_hit_rate += len(cite_id_intersection) / len(question_cite_ids)
        
        logger.warning(f"Total cite id hit rate: {total_cite_id_hit_rate/len(questions)}")
    

    async def evaluate_two_step(self , keyword_filter: bool) -> None:
        await self.qdrant_act_collection.client.create_payload_index(
            collection_name=self.qdrant_act_collection.collection_name,
            field_name='act_nro',
            field_schema="integer"
        )
        questions: List[Question] = self.get_questions_to_evaluate()

        total_act_hit_rate = 0
        total_cite_id_hit_rate = 0
        total_keyword_hit_rate = 0

        average_acts_per_first_search = 0
        average_cite_ids_per_first_search = 0
        average_keywords_per_first_search = 0

        act_vectors_returned = 100
        total_cite_id_hit_rate_second_search = 0
        

        for question in tqdm.tqdm(questions):
            keyword_filter = []
            question_related_acts = set([related_act.nro for related_act in question.relatedActs])
            question_cite_ids = set([(relation_data.nro,relation_data.id) for related_act in question.relatedActs for relation_data in related_act.relationData])
            question_keywords = set([(keyword.conceptId, keyword.instanceOfType) for keyword in question.keywords])

            
            question_vector = self.model.encode('zapytanie: ' + question.title, convert_to_tensor=False, show_progress_bar=False)
            related_questions = await self.qdrant_question_collection.search_questions_excluding_ids(limit=7, exclude_ids=[question.nro], vector=question_vector)
            
            results_related_acts = set()
            results_cite_ids = set()
            results_keywords = set()

            for related_question in related_questions:
                related_question = Question(**related_question.payload)
                for related_act in related_question.relatedActs:
                    results_related_acts.add(related_act.nro)
                    for relation_data in related_act.relationData:
                        results_cite_ids.add((relation_data.nro, relation_data.id))
                for keyword in related_question.keywords:
                    keyword_filter.append(keyword)
                    results_keywords.add((keyword.conceptId, keyword.instanceOfType))

            act_intersection = question_related_acts.intersection(results_related_acts)
            cite_id_intersection = question_cite_ids.intersection(results_cite_ids)
            keyword_intersection = question_keywords.intersection(results_keywords)

            average_acts_per_first_search += len(results_related_acts)
            average_cite_ids_per_first_search += len(results_cite_ids)
            average_keywords_per_first_search += len(results_keywords)

            hit_rate_acts = len(act_intersection) / len(question_related_acts)
            hit_rate_cite_ids = len(cite_id_intersection) / len(question_cite_ids)
            hit_rate_keywords = len(keyword_intersection) / len(question_keywords)

            total_act_hit_rate += hit_rate_acts
            total_cite_id_hit_rate += hit_rate_cite_ids
            total_keyword_hit_rate += hit_rate_keywords
            
            if keyword_filter:
                second_results = await self.qdrant_act_collection.search_acts_keyword_filtered(limit=act_vectors_returned, vector=question_vector, keywords=keyword_filter ,act_nros=list(results_related_acts))
            else:
                second_results = await self.qdrant_act_collection.search_acts_filtered(limit=act_vectors_returned, vector=question_vector, act_nros=list(results_related_acts))
            
            second_results_cite_ids = set()
            for cite_vector in second_results:
                cite_vector = ActVector(**cite_vector.payload)
                for node_id in cite_vector.node_ids:
                    second_results_cite_ids.add((cite_vector.act_nro, node_id))
                
            cite_id_intersection_second_search = question_cite_ids.intersection(second_results_cite_ids)
            total_cite_id_hit_rate_second_search += len(cite_id_intersection_second_search) / len(question_cite_ids)
                    


        logger.warning(f"********FIRST SEARCH********")
        logger.warning(f"Total act hit rate: {total_act_hit_rate/len(questions)}")
        logger.warning(f"Total cite id hit rate: {total_cite_id_hit_rate/len(questions)}")
        logger.warning(f"Total keyword hit rate: {total_keyword_hit_rate/len(questions)}")

        logger.warning(f"Average acts per first search: {average_acts_per_first_search/len(questions)}")
        logger.warning(f"Average cite ids per first search: {average_cite_ids_per_first_search/len(questions)}")
        logger.warning(f"Average keywords per first search: {average_keywords_per_first_search/len(questions)}")

        logger.warning(f"********SECOND SEARCH********")
        logger.warning(f"For second search, act vectors returned: {act_vectors_returned}")
        logger.warning(f"Total cite id hit rate second search: {total_cite_id_hit_rate_second_search/len(questions)}")
