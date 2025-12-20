import redis
import json
from typing import Optional, Any
from .config import settings

class Cache:
    def __init__(self):
        self.client = redis.from_url(settings.redis_url, decode_responses=True)

    def get(self, key: str) -> Optional[Any]:
        val = self.client.get(key)
        if val:
            try:
                return json.loads(val)
            except json.JSONDecodeError:
                return val
        return None

    def set(self, key: str, value: Any, expire: int = 300):
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        self.client.set(key, value, ex=expire)

    def delete(self, key: str):
        self.client.delete(key)

    def invalidate_prefix(self, prefix: str):
        # Use SCAN to find keys. Note: This can be slow on large datasets but fine for this scale.
        # Ideally use a set to track keys if performance is critical.
        keys = []
        cursor = '0'
        while cursor != 0:
            cursor, data = self.client.scan(cursor=cursor, match=f"{prefix}*", count=100)
            keys.extend(data)
        
        if keys:
            self.client.delete(*keys)

cache = Cache()
