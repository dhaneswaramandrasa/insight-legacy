# services/simple_service.py
from utilities.queue.simple_queue import queue_simple_task

def send_simple_task():
    return queue_simple_task()
