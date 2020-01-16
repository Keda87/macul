# Macul
Event driven worker consumer based on redis and utilize asyncio and [uvloop](https://github.com/MagicStack/uvloop) to achieve high concurrency.

There are already exists [celery](http://www.celeryproject.org/), [rq](https://python-rq.org/) and anything else in python community, 
but the goal of this project is to allow us to create worker as simple as possible and has agnostic producer.

## Usage Example:

__Producer:__

You can use any programming languages as long you `LPUSH` the event using key `macul:default:task`.

```bash
$ LPUSH macul:default:task '{"event": "SIGN_UP", "body": "{\"email\": "\john.doe@gmail.com"\, \"password\": \"test123$\"}"}'
```

Also the message is must have these two keys `event` with string value and `body` is JSON serializable or any primitive data type.

__Consumer:__
```python
from macul import Macul


app = Macul()


@app.consumer(event_name='testing')
async def worker_testing(message):
    print('Consumed by: testing')
    print(message)


@app.consumer(event_name='notification')
async def worker_notification(message):
    print('Consumed by: notification')
    print(message)


if __name__ == '__main__':
    app.start()
```

##### TODO:
- [ ] Backoff / Retry mechanism.
- [ ] Auto reconnection if the redis die.
- [ ] Unit test.
- [ ] Publish on [PyPI](https://pypi.org/).


### ___This project is still experimental and in heavy development.___.
