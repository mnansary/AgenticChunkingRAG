from agentchunking.dataLoader import get_current_data_splits
from agentchunking.segmentation import semantic_text_splitter
from loguru import logger
import time

if __name__ == "__main__":
    data, db = get_current_data_splits()
    if len(data) > 0:
        idx = 0
        while idx < len(data):
            row = data.iloc[idx]
            passage_id = row["id"]
            passage = row["text"]
            try:
                logger.info(f"Processing passage: {passage_id}")
                segments = semantic_text_splitter(passage, passage_id)
                db.segmentation_table_insert(segments)
                idx += 1  # proceed only if success
            except Exception as e:
                logger.error(f"Segmentation failed for passage {passage_id}: {e}")
                logger.info("Sleeping for 60 seconds before retrying...")
                time.sleep(60)  # wait before retrying
    else:
        logger.info("All data has been segmented. Rewriting can be initialized.")
