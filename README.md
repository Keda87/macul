# Macul
Event driven worker consumer based on redis and utilize asyncio and [uvloop](https://github.com/MagicStack/uvloop) to achieve high concurrency.

There are already exists [celery](http://www.celeryproject.org/), [rq](https://python-rq.org/) and anything else in python community, 
but the goal of this project is to allow us to create worker as simple as possible and has agnostic producer.

## Usage Example:

__Producer:__

You can use any programming languages as long you `LPUSH` the event using key with this pattern.

_Pattern: `<namespace>:<queue_name>:task`_

```bash
$ LPUSH macul:registration:task '{"event": "SIGN_UP", "body": "{\"email\": "\john.doe@gmail.com"\, \"password\": \"test123$\"}"}'
```

Also the message is must have these two keys `event` with string value and `body` is JSON serializable or any primitive data type.

__Consumer:__
```python
from macul import Macul


macul_app = Macul(event_name='SIGN_UP')  # You can custom the namespace using second optional argument.
macul_app.init_redis()                   # You can pass argument for the host, port, db and password.


@macul_app.consumer(queue_name='registration')  # queue_name = 'default' if not specified
async def worker_sign_up(data):
    # Process and do whatever you want with this data.
    pass


if __name__ == '__main__':
    macul_app.executor(worker_sign_up)

```

##### TODO:
- [ ] Backoff / Retry mechanism.
- [ ] Auto reconnection if the redis die.
- [ ] Unit test.
- [ ] Publish on [PyPI](https://pypi.org/).


### ___This project is still experimental and in heavy development.___.
