# buffr

lightweight event buffer that queues incoming traffic and saves it to the database in the background.

## the problem

when a busy site or app gets a sudden surge of traffic, writing every single click straight to the database can slow the server down or crash it.

## the solution

buffr sits in between. it accepts events instantly, drops them in a queue, and a worker saves them at a steady pace. users never wait and the database never gets overwhelmed.

## how it works

1. the client sends a json payload via http post, e.g. `{"event": "button_click", "userId": 123}`
2. the api validates it and pushes it into an in-memory queue, then returns `202` right away
3. a background worker pulls events off the queue one by one and saves them to sqlite

## quick start

```
pip install -r requirements.txt
uvicorn main:app --port 8000
```

## api

### `POST /events`

request body:

| field | type | required | notes |
|---|---|---|---|
| `event` | string | yes | 1 to 100 characters |
| `userId` | int or string | yes | |
| `properties` | object | no | max 4096 bytes when serialized |

example:

```
curl -X POST localhost:8000/events -H "content-type: application/json" -d '{"event":"button_click","userId":123,"properties":{"page":"/pricing"}}'
```

responses:

| status | meaning |
|---|---|
| `202` | event queued |
| `413` | properties too large |
| `422` | invalid payload |
| `503` | queue full, retry after the `Retry-After` header |

### `GET /stats`

returns counters and current queue depth:

```
{"accepted": 500, "rejected": 0, "saved": 500, "failed": 0, "queued": 0, "capacity": 10000}
```

### `GET /health`

returns `{"status": "ok"}`.

## configuration

constants at the top of `main.py`:

| constant | default | description |
|---|---|---|
| `DB_PATH` | `buffr.db` | sqlite file location |
| `QUEUE_MAX_SIZE` | `10000` | max events held in the queue before returning 503 |
| `WORKER_DELAY_SECONDS` | `0.005` | pause between saves, controls write pace |
| `SHUTDOWN_DRAIN_TIMEOUT_SECONDS` | `10` | how long to wait for the queue to empty on shutdown |
| `MAX_EVENT_NAME_LENGTH` | `100` | max length of the event name |
| `MAX_PROPERTIES_BYTES` | `4096` | max size of the properties object |

## database

events are stored in the `events` table:

| column | description |
|---|---|
| `id` | auto-increment primary key |
| `event` | event name |
| `user_id` | user id as text |
| `properties` | json string |
| `received_at` | unix time the api accepted it |
| `saved_at` | unix time the worker saved it |

## limitations

- the queue lives in memory, so events still queued are lost on a hard crash. a graceful shutdown drains the queue first
- one worker writing to sqlite, fine for an mvp but not for huge sustained throughput

## roadmap

- swap the in-memory queue for rabbitmq or redis for durability
- batch inserts in the worker
- api key auth
- postgres support
