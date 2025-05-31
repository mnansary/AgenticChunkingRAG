from agentchunking.constants import EMBDEEING_MODEL
from transformers import AutoTokenizer
from typing import List,Tuple
from agentchunking.llm.shortner import shorten_text

e5_tokenizer = AutoTokenizer.from_pretrained(EMBDEEING_MODEL)

def count_e5_tokens(text: str) -> int:
    tokens = e5_tokenizer.encode(text, add_special_tokens=True)
    return len(tokens)


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
        shortened = shorten_text(chunk)
        current_start=start
        start += len(shortened.split())
        segments.append({"passage_id":passage_id,"text":shortened, "start":current_start, "end":start - 1,"data":''})
    return segments
