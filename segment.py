from agentchunking.dataLoader import get_current_data_splits
from agentchunking.segmentation import semantic_text_splitter
from loguru import logger
if __name__=="__main__":
    data,db=get_current_data_splits()
    if len(data)>0:
        try:
            for idx,row in data.iterrows():
                passage_id=row["id"]
                passage=row["text"]
                logger.info(f"processing passage:{passage_id}")
                segments=semantic_text_splitter(passage,passage_id)
                db.segmentation_table_insert(segments)
        except Exception as e:
            logger.error(f"Segmentation Failed:{e}")
            raise e
    else:
        logger.info("All Data has been segmented, Rewriting can be initialized")
