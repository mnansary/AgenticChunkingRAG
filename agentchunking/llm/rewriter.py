from google.genai import types
from pydantic import BaseModel, Field
from typing import Optional,Any
import json 
from agentchunking.constants import REWRITER_GOOGLE_MODEL
# --------- Gemini Configuration ---------
class RewrittenPassage(BaseModel):
    rewritten_passage: str = Field(..., description="Self-contained, rewritten Bengali passage")

rewrite_gen_config = types.GenerateContentConfig(
    response_mime_type="application/json",
    response_schema=RewrittenPassage,
    temperature=0.3)  # Slight creativity for paraphrasing, but still factual
    
# --------- Prompt Template ---------
def build_prompt(topic: str, passage_heading: str, passage: str) -> str:
    return f"""
You are a passage rewriting assistant for Bengali government content.

Your task is to rewrite the following passage in Bengali while keeping the following rules:

1. Preserve all factual information and clarity.
2. Ensure the rewritten passage is fully self-contained.
3. Maintain a formal, clear, and neutral tone.
4. Use the topic and passage heading to provide context. THE PASSAGE MUST CLEARLY RELATE TO THE TOPIC.
5. Absoulte information like name,number,date,time,year must be preserved.
6. Reduce the number of words if it is too wordy but YOU CAN NOT LOOSE INFORMATION
7.The idea is that if a question is asked in about original passage it can not be missing in the rewriten passage.
8.Tone or words may not match but INFORMATION MUST BE PRESERVED

Topic: {topic}
Passage Heading: {passage_heading}
Original Passage: {passage}

Return the rewritten passage only.
"""


# --------- Main Rewrite Function ---------
def rewrite_passage(topic: str, heading: str, passage: str,client:Any) -> Optional[str]:
    try:
        prompt = build_prompt(topic, heading, passage)

        
        response = client.models.generate_content(
            model=REWRITER_GOOGLE_MODEL,
            contents=prompt,
            config=rewrite_gen_config,
        )
        rewriten=json.loads(response.text)['rewritten_passage']

        return rewriten

    except Exception as e:
        print("Error during generation:", e)
        return None
