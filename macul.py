import asyncio
import aioredis
import attr
import logging
import json
import os
import sys
import time
import uvloop

from functools import wraps
from typing import Any, Callable
from aioredis.errors import ConnectionClosedError


PID = os.getpid()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)

@attr.s
class Message:
    event = attr.ib()
    body = attr.ib()


class Macul:

    def __init__(self, event_name: str, namespace : str ='macul') -> None:
        self.redis = None
        self.namespace = namespace
        self.event_name = event_name
        self.queue_name = 'default'

    @property
    def queue_task_name(self) -> str:
        return f'{self.namespace}:{self.queue_name}:task'

    @property
    def queue_fail_name(self) -> str:
        return f'{self.namespace}:{self.queue_name}:fail'

    def init_redis(self, host='127.0.0.1', port='6379', db=0, password=None):
        self.redis = aioredis.create_redis_pool(
            f'redis://{host}:{port}',
            db=db,
            password=password,
        )

    def _parse_message(self, data: bytes) -> Message:
        raw_message = json.loads(data.decode('utf8'))
        return Message(event=raw_message['event'], body=raw_message['body'])

    def consumer(self, queue_name: str=None) -> Callable[[Callable], None]:
        self.queue_name = queue_name if queue_name is not None else None
        def wrapper(func):
            @wraps(func)
            async def wrapped(*args, **kwargs) -> None:
                print(f'Worker is listening "{self.queue_task_name}" on {PID}')
                redis_conn = await self.redis
                while True:
                    _, data = await redis_conn.brpop(self.queue_task_name)
                    message = self._parse_message(data)
                    try:
                        if message.event == self.event_name:
                            asyncio.create_task(func(message.body))
                    except KeyError:
                        logging.error('invalid payload')
                    except Exception:
                        data = json.dumps(data)
                        redis.lpush(self.queue_fail_name, data)
                        logging.info('Failed event moved to fail queue')
            return wrapped
        return wrapper

    def executor(self, func: Callable[[Any], None]) -> None:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        event_loop = asyncio.get_event_loop()
        try:
            asyncio.ensure_future(func())
            event_loop.run_forever()
        except ConnectionClosedError:
            logging.error('Connection closed')
        except KeyboardInterrupt:
            sys.exit(1)
        finally:
            event_loop.close()

    def __repr__(self):
        return f'<{self.__class__.name}>'
