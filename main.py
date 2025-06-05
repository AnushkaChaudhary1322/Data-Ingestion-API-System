import asyncio
import time
import uuid
from enum import Enum
from queue import PriorityQueue
from typing import Dict, List, Literal, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

# --- Configuration Constants ---
BATCH_SIZE = 3
RATE_LIMIT_SECONDS = 5
MAX_ID_VALUE = 10**9 + 7

# --- Enums ---
class Priority(str, Enum):
    """Defines the priority levels for ingestion requests."""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

class BatchStatus(str, Enum):
    """Defines the possible statuses for individual batches."""
    YET_TO_START = "yet_to_start"
    TRIGGERED = "triggered"
    COMPLETED = "completed"