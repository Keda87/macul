import asyncio
import json
import logging
import os
import signal
from functools import wraps
from typing import Any, Callable

import aioredis
import attr
import uvloop

PID = os.getpid()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


async def shutdown(signal, loop):
    logging.info(f'Received exit signal {signal.name}...')
    logging.info('Closing redis connections')
    tasks = [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
    ]

    [task.cancel() for task in tasks]

    logging.info(f'Cancelling {len(tasks)} outstanding tasks')
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


@attr.s
class Message:
    event = attr.ib()
    body = attr.ib()


class Macul:

    def __init__(self, event_name: str, namespace: str = 'macul') -> None:
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

    def consumer(self, queue_name: str = None) -> Callable[[Callable], None]:
        if queue_name is not None:
            self.queue_name = queue_name
        def wrapper(func):
            @wraps(func)
            async def wrapped(*args, **kwargs) -> None:
                print(f'Worker is listening "{self.queue_task_name}" on PID: {PID}')
                redis_conn = await self.redis
                while True:
                    try:
                        _, data = await redis_conn.brpop(self.queue_task_name)
                        message = self._parse_message(data)
                        if message.event == self.event_name:
                            asyncio.create_task(func(message.body))
                    except KeyError:
                        logging.error('invalid payload')
                    except Exception:
                        data = json.dumps(data)
                        redis_conn.lpush(self.queue_fail_name, data)
                        logging.info('Failed event moved to fail queue')
            return wrapped
        return wrapper

    def executor(self, func: Callable[[Any], None]) -> None:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        event_loop = asyncio.get_event_loop()

        signals = [signal.SIGHUP, signal.SIGTERM, signal.SIGINT]
        for sign in signals:
            event_loop.add_signal_handler(
                sign, 
                lambda s=sign: asyncio.create_task(shutdown(sign, event_loop))
            )

        try:
            asyncio.ensure_future(func())
            event_loop.run_forever()
        finally:
            event_loop.close()

    def __repr__(self):
        print(self.__class__)
        return f'<{self.__class__.__name__}>'
