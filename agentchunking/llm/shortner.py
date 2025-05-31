from google.genai import types
from pydantic import BaseModel, Field
from typing import List, Tuple
import json
from agentchunking.constants import GOOGLE_MODEL
from agentchunking.clientManagement import get_available_client,google_clients

#------------------------------------------------------------------------------------------------------------------
# --------- Gemini Configuration ---------
class CopiedPassage(BaseModel):
    new_passage: str = Field(..., description="Self-contained, copied Bengali passage.")

copy_config = types.GenerateContentConfig(
    response_mime_type="application/json",
    response_schema=CopiedPassage,
    temperature=0.0
)

passage_prompt_google = (
    "Copy the following text until you reach a natural breaking point, "
    "such as the end of a section or topic:\n\n{passage}"
)

def shorten_text_goole_api(text: str, client) -> str:
    prompt = passage_prompt_google.format(passage=text)
    response = client.models.generate_content(
        model=GOOGLE_MODEL,
        contents=prompt,
        config=copy_config,
    )
    return json.loads(response.text)["new_passage"].strip()
#------------------------------------------------------------------------------------------------------------------

#------------------------------------------------------------------------------------------------------------------
passage_prompt_llama = """
You are given a Bengali passage. Your task is to copy and return a self-contained portion of it, starting from the beginning and stopping at a natural breaking point.

A "natural breaking point" means one of the following:
- The end of a paragraph that completes a full idea or topic.
- The end of a sentence that concludes a specific thought or example.
- A clear section boundary, such as a heading or change in subject.

Do NOT cut off sentences midway. Do NOT return partial thoughts. Stop only after a complete idea has been conveyed.

Return the result as a JSON object with a single field "new_passage", like this:
{{
  "new_passage": "Copied Bengali text goes here..."
}}

Here is the original passage to process:
{passage}
"""

def shorten_text_llama(text: str, client) -> str:
    prompt = passage_prompt_llama.format(passage=text)
    
    completion = client.chat.completions.create(extra_body={},model="meta-llama/llama-3.3-8b-instruct:free",
                                                messages=[{"role": "user","content": prompt}])
    # Generate Q&A using the LLM
    result = completion.choices[0].message.content
    
    return json.loads(result.text)["new_passage"].strip()
#------------------------------------------------------------------------------------------------------------------
def shorten_text(chunk: str) -> str:
    client_wrapper = get_available_client(google_clients)
    client = client_wrapper.use()
    return shorten_text_goole_api(chunk, client)
