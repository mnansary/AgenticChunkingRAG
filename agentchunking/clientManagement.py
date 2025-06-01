from agentchunking.constants import GOOGLE_APIS_CSV
from google import genai
import pandas as pd 
from datetime import datetime, timedelta
from typing import List
import random
from loguru import logger
from collections import deque

class APIClientWrapper:
    def __init__(self, client, daily_limit: int, rpm_limit: int):
        self.client = client
        self.daily_limit = daily_limit
        self.rpm_limit = rpm_limit
        self.calls_made = 0
        self.last_reset = datetime.now().date()
        self.request_timestamps = deque()  # Tracks timestamps of requests within last minute

    def is_available(self):
        # Reset daily count at the start of a new day
        if self.last_reset != datetime.now().date():
            self.calls_made = 0
            self.last_reset = datetime.now().date()
            self.request_timestamps.clear()

        # Remove timestamps older than 1 minute
        current_time = datetime.now()
        while self.request_timestamps and (current_time - self.request_timestamps[0]) > timedelta(minutes=1):
            self.request_timestamps.popleft()

        # Check both daily and per-minute limits
        return self.calls_made < self.daily_limit and len(self.request_timestamps) < self.rpm_limit

    def use(self):
        if not self.is_available():
            raise RuntimeError("Quota exceeded for this client (daily or RPM limit).")
        self.calls_made += 1
        self.request_timestamps.append(datetime.now())
        return self.client

def create_wrapped_clients_google(rpd,rpm):
    apis = pd.read_csv(GOOGLE_APIS_CSV)["api"].tolist()
    return [APIClientWrapper(genai.Client(api_key=key), rpd, rpm) for key in apis]

def get_available_client(wrapped_clients: List[APIClientWrapper]):
    available_clients = [c for c in wrapped_clients if c.is_available()]
    if not available_clients:
        logger.error("No available clients: all quotas used.")
        raise RuntimeError("No available clients: all quotas used.")
    return random.choice(available_clients)