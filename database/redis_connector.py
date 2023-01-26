import json

import redis

from constants import REDIS_PASS, REDIS_PORT, REDIS_QUEUE, REDIS_URL

rdb = redis.Redis(host=REDIS_URL, port=int(REDIS_PORT), password=REDIS_PASS, db=0)


def push_to_redis_queue(data: dict) -> None:
    rdb.rpush(REDIS_QUEUE, json.dumps(data))
