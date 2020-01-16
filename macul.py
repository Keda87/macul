import asyncio
import json
import logging
import os
import signal
from functools import wraps

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
    tasks = [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
    ]

    [task.cancel() for task in tasks]

    logging.info(f'Cancelling {len(tasks)} outstanding tasks')
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


def parse_message(data: bytes):
    raw_message = json.loads(data.decode('utf8'))
    return Message(event=raw_message['event'], body=raw_message['body'])


@attr.s
class Message:
    event = attr.ib()
    body = attr.ib()


class Macul:
    __slots__ = ['jobs', 'redis', 'queue_name']

    def __init__(self, host='127.0.0.1', port='6379', db=0, password=None):
        self.jobs = {}
        self.queue_name = 'macul:default:task'
        self.redis = aioredis.create_redis_pool(
            f'redis://{host}:{port}',
            db=db,
            password=password,
        )

    def consumer(self, event_name: str):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                self.jobs[event_name] = func
            return wrapper()
        return decorator

    async def _worker_start(self):
        try:
            redis_conn = await self.redis
            while True:
                _, data = await redis_conn.brpop(self.queue_name)
                message = parse_message(data)
                try:
                    func = self.jobs[message.event]
                    asyncio.create_task(func(message.body))
                except KeyError:
                    logging.error(f'Unregistered event: {message.event}')
        except ConnectionError:
            logging.error('Redis is not connected.')

    def start(self):
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        event_loop = asyncio.get_event_loop()

        signals = [signal.SIGHUP, signal.SIGTERM, signal.SIGINT]
        for sign in signals:
            event_loop.add_signal_handler(
                sign,
                lambda s=sign: asyncio.create_task(shutdown(sign, event_loop))
            )

        try:
            asyncio.ensure_future(self._worker_start())
            event_loop.run_forever()
        finally:
            event_loop.close()

    def __repr__(self):
        return f'<{self.__class__.__name__}>'
