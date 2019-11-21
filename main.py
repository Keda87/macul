import asyncio
import aioredis
import json
import os
import sys
import time
import uvloop

from aioredis.errors import ConnectionClosedError


QUEUE = os.environ.get('QUEUE_NAME')
REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PASS = os.environ.get('REDIS_PASS')
REDIS_PORT = os.environ.get('REDIS_PORT')
REDIS_DB = os.environ.get('REDIS_DB')

PID = os.getpid()
QUEUE_TASK = f'macul:{QUEUE}:task'
QUEUE_FAIL = f'macul:{QUEUE}:fail'


async def worker(redis):
    print(f'Worker {PID} is listening on "{QUEUE_TASK}"')
    while True:
        await asyncio.sleep(1)
        _, data = await redis.brpop(QUEUE_TASK)
        data = json.loads(data.decode('utf8'))
        try:
            event_name = data['event']
            event_data = data['data']
            print(event_name)
            print(event_data)
            print('------------------')
        except KeyError:
            print('invalid payload, just ignore it...')
        except Exception:
            data = json.dumps(data)
            redis.lpush(QUEUE_FAIL, data)


async def init():
    redis = await aioredis.create_redis_pool(
        f'redis://{REDIS_HOST}:{REDIS_PORT}',
        password=REDIS_PASS,
        db=int(REDIS_DB),
    )

    await worker(redis)


def main():
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    event_loop = asyncio.get_event_loop()
    try:
        asyncio.ensure_future(init())
        event_loop.run_forever()
    except ConnectionClosedError:
        print('Connection closed...')
    except KeyboardInterrupt:
        sys.exit(1)
    finally:
        event_loop.close()


if __name__ == '__main__':
    main()
