from agentchunking.constants import EMBDEEING_MODEL,COPIER_MAX_ALLOWED_RPM
from transformers import AutoTokenizer
from typing import List,Tuple
from agentchunking.llm.shortner import shorten_text
from time import sleep
from loguru import logger
import time
from datetime import datetime, timedelta

e5_tokenizer = AutoTokenizer.from_pretrained(EMBDEEING_MODEL)

def count_e5_tokens(text: str) -> int:
    tokens = e5_tokenizer.encode(text, add_special_tokens=True)
    return len(tokens)



# Initialize counters and timer
copier_request_count = 0
rpm_window_start = time.time()

def enforce_copier_rpm():
    global copier_request_count, rpm_window_start

    current_time = time.time()
    elapsed = current_time - rpm_window_start

    if copier_request_count >= COPIER_MAX_ALLOWED_RPM:
        sleep_time = max(0, 60 - elapsed)
        logger.warning(f"Reached COPIER RPM limit. Sleeping for {sleep_time:.2f} seconds.")
        time.sleep(sleep_time)
        # Reset after sleep
        rpm_window_start = time.time()
        copier_request_count = 0

    copier_request_count += 1


def semantic_text_splitter(passage: str,
                           passage_id:str,
                           max_tokens: int = 450,
                           step_words: int = 10
) -> List[Tuple[str, int, int]]:
    words = passage.split()
    total = len(words)
    start = 0
    segments: List[Tuple[str, int, int]] = []

    while start < total:
        end = start
        # grow window in chunks of step_words until token limit
        while end < total and count_e5_tokens(" ".join(words[start:end + 1])) <= max_tokens:
            end += step_words
        # if we overshot, back off one chunk
        if end > start and count_e5_tokens(" ".join(words[start:end])) > max_tokens:
            end -= step_words
        # ensure at least one word
        if end == start:
            end = start + 1

        chunk = " ".join(words[start:end])
        enforce_copier_rpm()
        shortened = shorten_text(chunk)
        #sleep(9.5)
        current_start=start
        start += len(shortened.split())
        segments.append({"passage_id":passage_id,"text":shortened, "start":current_start, "end":start - 1,"data":''})
    return segments
