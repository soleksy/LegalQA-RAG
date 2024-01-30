import os
import json
import dotenv
import logging
import asyncio 

from sessionmanager.session_manager import SessionManager

from etl.extract.extractquestions.extract_questions_domain import ExtractQuestionsDomain
from etl.extract.extractacts.extract_acts import ExtractActs
from etl.extract.extractkeywords.extract_keywords import ExtractKeywords

from etl.transform.transformquestions.transform_questions import TransformQuestions
from etl.transform.transformkeywords.transform_keywords import TransformKeywords
from etl.transform.transformacts.transform_acts import TransformActs

from etl.common.questionindex.transformed_question_index import TransformedQuestionIndex

from etl.validate.validate import Validate


dotenv.load_dotenv()

EMAIL = os.getenv('EMAIL')
PASSWORD = os.getenv('PASSWORD')
DOMAINS = os.getenv('DOMAINS')

#Ignore warnings and errors, they are being handled by tenacity
async def main():

    session_manager = SessionManager(EMAIL, PASSWORD)
    extract = ExtractQuestionsDomain(session_manager)
    transform = TransformQuestions(session_manager)
    transformed_question_index = TransformedQuestionIndex()
    domains = json.loads(DOMAINS)

    domains = None #Cannot expose this to the public
    
    logging.info(f'Extracting questions')
    extracted_questions = await extract.get_all_questions(domains=domains)
    logging.info(f'Extracted {len(extracted_questions)} questions')

    logging.info(f'Transforming questions')
    transformed_questions = await transform.transform_questions(extracted_questions, domains=domains)
    logging.info(f'Transformed {len(transformed_questions)} questions')

    act_nros_from_questions = transformed_question_index._get_act_nros(domains=domains)
    logging.info(f'Extracting acts')
    extractor = ExtractActs(session_manager)
    await extractor.get_acts(act_nros=act_nros_from_questions)
    logging.info(f'Extracted acts')

    logging.info(f'Extracting keywords')
    keyword_extractor = ExtractKeywords(session_manager)
    keywords_from_questions = transformed_question_index._get_keywords_ids(domains=domains) 
    await keyword_extractor.get_keywords(keywords = keywords_from_questions)
    logging.info(f'Extracted keywords')

    logging.info(f'Transforming keywords')
    transform_keywords = TransformKeywords()
    transform_keywords.transform_not_indexed_keywords()
    logging.info(f'Transformed keywords')
    #There are errors in the keywords, api. Needs a complex parser to fix them. 1 keyword to fix look for 'all()' in units['id'].
    #TODO add at least an automatic check for files with an api error.
    #After cleaning run the following:
    
    '''
    logging.info(f'Transforming acts')
    transform_acts = TransformActs()
    transform_acts.transform_acts()
    logging.info(f'Transformed acts')

    logging.info(f'Validating')
    validate = Validate()
    validate.validate_dataset()
    logging.info(f'Validation complete')
    '''
    

if __name__ == '__main__':
    asyncio.run(main())
