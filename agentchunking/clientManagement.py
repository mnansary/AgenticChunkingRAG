from agentchunking.constants import GOOGLE_APIS_CSV,MAX_ALLOWED_RPD
from google import genai
# from openai import OpenAI
import pandas as pd 
from datetime import datetime
from typing import List
import random
from loguru import logger

class APIClientWrapper:
    def __init__(self, client, daily_limit: int):
        self.client = client
        self.daily_limit = daily_limit
        self.calls_made = 0
        self.last_reset = datetime.now().date()

    def is_available(self):
        # Reset count at the start of a new day
        if self.last_reset != datetime.now().date():
            self.calls_made = 0
            self.last_reset = datetime.now().date()
        return self.calls_made < self.daily_limit

    def use(self):
        if not self.is_available():
            raise RuntimeError("Quota exceeded for this client.")
        self.calls_made += 1
        return self.client

def create_wrapped_clients_google():
    apis = pd.read_csv(GOOGLE_APIS_CSV)["api"].tolist()
    return [APIClientWrapper(genai.Client(api_key=key), MAX_ALLOWED_RPD) for key in apis]

# def create_wrapped_clients_llama():
#     apis = pd.read_csv(OPENAI_APIS_CSV)["api"].tolist()
#     return [APIClientWrapper(OpenAI(base_url="https://openrouter.ai/api/v1", api_key=key), 150) for key in apis]

def get_available_client(wrapped_clients: List[APIClientWrapper]):
    available_clients = [c for c in wrapped_clients if c.is_available()]
    if not available_clients:
        logger.error("No available clients: all quotas used.")
        raise "No available clients: all quotas used."
    return random.choice(available_clients)

google_clients=create_wrapped_clients_google()
logger.info(f"Created Google Clinets:{len(google_clients)}")
# llama_clients=create_wrapped_clients_llama()
# logger.info(f"Created LLama Clients:{len(llama_clients)}")