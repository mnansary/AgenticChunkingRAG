from agentchunking.constants import GOOGLE_APIS_CSV
from google import genai
import pandas as pd
from datetime import datetime, timedelta
from typing import List
import random
import time
from loguru import logger
from collections import deque


class APIClientWrapper:
    def __init__(self, client, daily_limit: int, rpm_limit: int):
        self.client = client
        self.daily_limit = daily_limit
        self.rpm_limit = rpm_limit
        self.calls_made = 0
        self.last_reset = datetime.now().date()
        self.request_timestamps = deque()

    def is_available(self):
        # Reset daily count at new day
        if self.last_reset != datetime.now().date():
            self.calls_made = 0
            self.last_reset = datetime.now().date()
            self.request_timestamps.clear()

        # Clear timestamps older than 1 minute
        current_time = datetime.now()
        while self.request_timestamps and (current_time - self.request_timestamps[0]) > timedelta(minutes=1):
            self.request_timestamps.popleft()

        # Check limits
        return self.calls_made < self.daily_limit and len(self.request_timestamps) < self.rpm_limit

    def use(self):
        if not self.is_available():
            raise RuntimeError("Quota exceeded for this client (daily or RPM limit).")
        self.calls_made += 1
        self.request_timestamps.append(datetime.now())
        return self.client


class RoundRobinClientManager:
    def __init__(self, clients: List[APIClientWrapper]):
        self.clients = clients
        self.index = 0  # Round-robin pointer

    def get_next_available_client(self):
        start_index = self.index
        num_clients = len(self.clients)

        for _ in range(num_clients):
            client = self.clients[self.index]
            self.index = (self.index + 1) % num_clients
            if client.is_available():
                return client

        # No client available: wait and retry
        logger.warning("All clients exceeded RPM. Waiting 60 seconds to retry.")
        time.sleep(60)

        # Retry once after cooldown
        for _ in range(num_clients):
            client = self.clients[self.index]
            self.index = (self.index + 1) % num_clients
            if client.is_available():
                return client

        # If still unavailable, raise error
        logger.error("No available clients even after cooldown.")
        raise RuntimeError("No available clients after waiting for RPM reset.")

    def get_client(self):
        client_wrapper = self.get_next_available_client()
        return client_wrapper.use()


def create_wrapped_clients_google(rpd, rpm):
    apis = pd.read_csv(GOOGLE_APIS_CSV)["api"].tolist()
    wrapped_clients = [APIClientWrapper(genai.Client(api_key=key), rpd, rpm) for key in apis]
    return RoundRobinClientManager(wrapped_clients)
