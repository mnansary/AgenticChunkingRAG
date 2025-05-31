from agentchunking.utils.filehelpers import config_loader
from agentchunking.database.manager import SQLDatabaseManager
from agentchunking.constants import (MAX_TOKEN_PASSAGE_TO_USE_AS_IT_IS,
                                     DB_CONFIG_PATH,
                                     LLM_MODEL,
                                     EMBDEEING_MODEL)
import re
from transformers import AutoTokenizer
from loguru import logger

# global 
e5_tokenizer = AutoTokenizer.from_pretrained(EMBDEEING_MODEL)  # or "intfloat/multilingual-e5-large"
llama_tokenizer = AutoTokenizer.from_pretrained(LLM_MODEL)


# helpers
def count_e5_tokens(text):
    # Tokenize the text and count tokens
    tokens = e5_tokenizer.encode(text,add_special_tokens=True)
    return len(tokens)



def count_llama_tokens(text):
    # Tokenize the text and count tokens
    tokens = llama_tokenizer.encode(text)
    return len(tokens)



def clear_tag_text(text):
    if "passage_heading:" in text:
        return text.split('\n', 1)[-1]
    else:
        return text
    
def clean_bangla_text(text):
    # Remove trailing underscores, hyphens, newlines, pipes
    text = re.sub(r'[_|-|\n]+$', '', text.rstrip())
    # Keep Bangla characters (U+0980 to U+09FF), alphanumeric, and spaces; remove others
    text = re.sub(r'[^\u0980-\u09FFa-zA-Z0-9\s]', '', text)
    # Replace multiple spaces with a single space
    text = re.sub(r'\s+', ' ', text).strip()
    return text



def get_current_data_splits():
    try:
        logger.info("# load database")
        db_config = config_loader(DB_CONFIG_PATH)
        db = SQLDatabaseManager(db_config)

        logger.info("# load data")
        data=db.annotation_table.select_columns(columns=['annotation_data_id','url', 'text', 'site_name', 'passage_heading'])
        data.rename(columns={'annotation_data_id': 'id',"site_name":"topic","passage_heading":"heading"}, inplace=True)

        inserted=db.segmentation_table.select()
        inserted_ids=[pid for pid in inserted.passage_id.unique()]
        logger.info(f"# found already inserted ids:{len(inserted_ids)}")
        data=data[~data["id"].isin(inserted_ids)]

        logger.info("# clear text from tags")
        data['text'] = data['text'].apply(clear_tag_text)
        data = data[data['text'].str.len() > 0]

        # data['text'] = data['text'].apply(clean_bangla_text)
        # data = data[data['text'].str.len() > 0]

        logger.info('# process heading')
        data['heading'] = data['heading'].apply(clean_bangla_text)
        data = data[data['heading'].str.len() > 0]
        
        logger.info('# process topic')
        data["topic"] = data["topic"].apply(clean_bangla_text)
        data = data[data['topic'].str.len() > 0]
        
        logger.info('# get word count')
        data["word_count"]=data["text"].apply(lambda x: len(x.split()))
        data = data[data.word_count>10]

        logger.info('# clear data')
        data = data.dropna(subset=['text', 'heading', 'topic'])
        
        logger.info('# get tokens')
        data["llama_token_count"] = data['text'].apply(count_llama_tokens)
        data["e5_token_count"] = data['text'].apply(count_e5_tokens)
        data.reset_index(drop=True, inplace=True)

        logger.info('# create split')
        data["use_as_it_is"]=data.apply(lambda x: True  if x["e5_token_count"]<=MAX_TOKEN_PASSAGE_TO_USE_AS_IT_IS else False,axis=1)
        unchanged=data.loc[data.use_as_it_is==True]
        changed=data.loc[data.use_as_it_is!=True]
        
        if len(unchanged)>0:
            logger.info("Inseting the segmentation data ")
            unchanged=unchanged[["id","text"]]
            unchanged.rename(columns={"id":"passage_id"},inplace=True)
            unchanged["start"]=0
            unchanged["data"]=""
            unchanged["end"]=unchanged["text"].apply(lambda x: len(x.split())) 
            list_of_dicts = unchanged.to_dict(orient='records')
            db.segmentation_table_insert(list_of_dicts)
        changed.reset_index(drop=True,inplace=True)
        return changed,db
    except Exception as e:
        logger.error(f"Error in getting current split data:{e}")
        return None