# @hapi/sse

SSE (Server-Sent Events) plugin for hapi. WHATWG spec-compliant with subscription-based pub/sub, event replay, backpressure, built-in stats, and lifecycle hooks.

## Install

```
npm install @hapi/sse
```

Peer dependencies: `@hapi/hapi` ^21, `@hapi/boom` ^10.

## Quick Start

```typescript
import Hapi from '@hapi/hapi';
import { SsePlugin } from '@hapi/sse';

const server = Hapi.server({ port: 3000 });

await server.register({ plugin: SsePlugin });

server.sse.subscription('/events');

await server.start();

// Publish from anywhere
await server.sse.publish('/events', { msg: 'hello' }, { event: 'chat' });
```

## Plugin Options

```typescript
await server.register({
    plugin: SsePlugin,
    options: {
        retry: 2000,                            // retry interval in ms (default: 2000, null to disable)
        keepAlive: { interval: 15_000 },         // keep-alive comment interval (default: 15s, false to disable)
        headers: { 'X-Custom': 'value' },        // extra headers on every SSE response
        backpressure: { maxBytes: 65536, strategy: 'drop' },  // optional
        hooks: { ... },                          // optional, see Hooks section
    },
});
```

## API

### `server.sse.subscription(path, config?)`

Registers a subscription route. Clients connect via `GET <path>`.

```typescript
server.sse.subscription('/events/{channel}', {
    auth: 'jwt',
    retry: 5000,
    keepAlive: { interval: 10_000 },
    filter: async (path, message, { credentials, params, internal }) => {
        if (params.channel !== internal.targetChannel) {
            return false; // don't deliver
        }
        return { override: { ...message, filtered: true } }; // or transform
    },
    onSubscribe: async (session, path, params) => {},
    onUnsubscribe: (session, path, params) => {},
    onReconnect: async (session, path, params) => {},
    replay: new FiniteReplayer({ size: 100 }), // optional, see Replay section
});
```

**Config options:**

| Option          | Type                                                               | Description                                                                                     |
| --------------- | ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------- |
| `auth`          | `RouteOptions['auth']`                                             | hapi auth config for the route                                                                  |
| `retry`         | `number \| null`                                                   | Override plugin-level retry                                                                     |
| `keepAlive`     | `{ interval: number } \| false`                                    | Override plugin-level keep-alive                                                                |
| `filter`        | `(path, message, opts) => boolean \| { override } \| Promise<...>` | Per-session delivery filter                                                                     |
| `onSubscribe`   | `(session, path, params) => void \| Promise<void>`                 | Fires before SSE headers are sent. Throwing a Boom error returns that HTTP error to the client. |
| `onUnsubscribe` | `(session, path, params) => void`                                  | Fires on client disconnect                                                                      |
| `onReconnect`   | `(session, path, params) => void \| Promise<void>`                 | Fires when `Last-Event-ID` is present (after replay). Errors close the session gracefully.      |
| `replay`        | `Replayer`                                                         | Replay provider for automatic reconnection replay                                               |

### `server.sse.publish(path, data, opts?)`

Publishes an event to all matching subscribers. Returns the number of sessions that received the event.

```typescript
const delivered = await server.sse.publish(
    '/events/news',
    { headline: '...' },
    {
        event: 'breaking',
        id: 'evt-42',
        internal: { targetChannel: 'news' }, // passed to filter
        matchMode: 'literal', // 'pattern' (default) or 'literal'
    },
);

console.log(`Delivered to ${delivered} sessions`);
```

**`matchMode`:**

- `'pattern'` (default) — delivers to all sessions on a matching subscription pattern (e.g. `/events/{channel}`)
- `'literal'` — only delivers to sessions whose actual connected path equals `path` exactly. Useful for parameterized subscriptions where you want to target `/events/news` but not `/events/sport`.

**Note:** Only events published with an explicit `id` are recorded by the replayer. Events without an `id` are delivered but not stored for replay.

### `server.sse.broadcast(data, opts?)`

Sends an event to every connected session across all subscriptions. Returns the delivery count.

```typescript
const count = await server.sse.broadcast({ type: 'maintenance' }, { event: 'system' });
```

### `server.sse.eachSession(fn, opts?)`

Iterates over connected sessions. Optionally filter by subscription pattern.

```typescript
await server.sse.eachSession(
    async (session) => {
        session.push({ ping: true });
    },
    { subscription: '/events' },
);
```

### `server.sse.subscriptions()`

Returns a snapshot of all registered subscriptions with active session counts.

```typescript
const subs = server.sse.subscriptions();
// [{ pattern: '/events', activeSessions: 3 }, { pattern: '/chat/{room}', activeSessions: 12 }]
```

### `server.sse.closeSessions(pattern)`

Closes all sessions for a specific subscription pattern.

```typescript
server.sse.closeSessions('/events'); // close all /events sessions
server.sse.closeSessions('/chat/{room}'); // close all chat sessions
```

### `server.sse.sessionCount`

Total number of active sessions across all subscriptions.

```typescript
console.log(server.sse.sessionCount); // 42
```

### `server.sse.stats()`

Returns built-in metrics tracked by the plugin. No configuration needed.

```typescript
const stats = server.sse.stats();
// {
//     totalConnections: 150,
//     totalDisconnections: 108,
//     totalPublishes: 5230,
//     totalBroadcasts: 12,
//     totalEventsDelivered: 48700,
//     activeSessions: 42,
// }
```

| Stat                   | Description                                                         |
| ---------------------- | ------------------------------------------------------------------- |
| `totalConnections`     | Cumulative subscription connections since server start              |
| `totalDisconnections`  | Cumulative disconnections since server start                        |
| `totalPublishes`       | Number of `publish()` calls                                         |
| `totalBroadcasts`      | Number of `broadcast()` calls                                       |
| `totalEventsDelivered` | Sum of all individual event deliveries across publish and broadcast |
| `activeSessions`       | Current connected session count (same as `sessionCount`)            |

## Session

The `Session` object represents a single SSE connection.

```typescript
session.push(data, event?, id?)   // Send an event. Returns boolean (false if dropped/closed).
session.comment(text?)            // Send a comment (invisible to EventSource)
session.close()                   // End the connection
session.isOpen                    // true if connection is still active
session.connectedAt               // Unix timestamp (ms) when the session was created
session.lastEventId               // Value of Last-Event-ID header (empty string if absent)
session.request                   // The original hapi Request object
```

**Metadata** — attach arbitrary key-value data to a session:

```typescript
session.set('userId', 42);
session.get('userId'); // 42
session.has('userId'); // true
session.delete('userId'); // true
```

Metadata persists for the lifetime of the session. Useful for tagging sessions in `onSubscribe` and reading in filters or `eachSession`.

## Custom Handler Mode

For full control over the stream (e.g. AI token streaming), use the handler decorator instead of subscriptions:

```typescript
server.route({
    method: 'GET',
    path: '/stream',
    handler: {
        sse: {
            stream: async (request, session) => {
                for (const token of tokens) {
                    session.push({ token }, 'token');
                }
                session.close();
            },
            retry: 3000, // override plugin-level retry
            keepAlive: { interval: 10_000 }, // override plugin-level keep-alive
            headers: { 'X-Stream': 'true' }, // override plugin-level headers
            backpressure: { maxBytes: 32768, strategy: 'close' },
        },
    },
});
```

**Handler options:**

| Option         | Type                                          | Description                                                                       |
| -------------- | --------------------------------------------- | --------------------------------------------------------------------------------- |
| `stream`       | `(request, session) => void \| Promise<void>` | Required. Called after SSE headers are sent. Errors close the session gracefully. |
| `retry`        | `number \| null`                              | Override plugin-level retry (default: inherits from plugin)                       |
| `keepAlive`    | `{ interval: number } \| false`               | Override plugin-level keep-alive (default: inherits from plugin)                  |
| `headers`      | `Record<string, string>`                      | Override plugin-level headers (default: inherits from plugin)                     |
| `backpressure` | `BackpressureOptions`                         | Override plugin-level backpressure (default: inherits from plugin)                |

## Event Replay

Automatic replay of missed events on client reconnection. When a client sends `Last-Event-ID`, the replayer pushes missed events before `onReconnect` fires.

Only events published with an explicit `id` are recorded. Events without an `id` are delivered but not stored for replay — this prevents the buffer from filling with unaddressable entries.

Two built-in replayers:

### FiniteReplayer

Fixed-size ring buffer. O(1) append, linear scan for replay.

```typescript
import { FiniteReplayer } from '@hapi/sse';

const replayer = new FiniteReplayer({ size: 100, autoId: true });

server.sse.subscription('/events', { replay: replayer });
```

### ValidReplayer

Time-based expiry with periodic garbage collection.

```typescript
import { ValidReplayer } from '@hapi/sse';

const replayer = new ValidReplayer({ ttl: 60_000, autoId: true });

server.sse.subscription('/events', { replay: replayer });
```

Call `replayer.stop()` to clear the GC timer (handled automatically on server stop).

**Options:**

| Option   | Type      | Description                                                  |
| -------- | --------- | ------------------------------------------------------------ |
| `size`   | `number`  | (FiniteReplayer) Max entries to keep                         |
| `ttl`    | `number`  | (ValidReplayer) Entry lifetime in ms                         |
| `autoId` | `boolean` | Auto-generate sequential IDs when events have no explicit ID |

**Custom replayer** — implement the `Replayer` interface:

```typescript
import type { Replayer, ReplayEntry } from '@hapi/sse';

class RedisReplayer implements Replayer {
    record(entry: ReplayEntry): void {
        /* store to Redis stream */
    }
    replay(lastEventId: string): ReplayEntry[] {
        /* read from Redis */
    }
    stop?(): void {
        /* cleanup */
    }
}
```

## Backpressure

Protects against slow consumers accumulating unbounded memory. Uses Node's `writableLength` to accurately measure bytes queued in the kernel buffer. Configurable at plugin level or per-handler.

```typescript
// Plugin level — applies to all subscription sessions
await server.register({
    plugin: SsePlugin,
    options: {
        backpressure: { maxBytes: 65536, strategy: 'close' },
    },
});

// Handler level — overrides plugin level
server.route({
    method: 'GET',
    path: '/stream',
    handler: {
        sse: {
            stream: async (req, session) => { ... },
            backpressure: { maxBytes: 32768, strategy: 'drop' },
        },
    },
});
```

**Strategies:**

| Strategy  | Behavior                                                |
| --------- | ------------------------------------------------------- |
| `'close'` | Closes the session when pending bytes exceed `maxBytes` |
| `'drop'`  | Silently drops the event but keeps the session open     |

When backpressure triggers, `session.push()` returns `false`.

## Hooks

Optional lifecycle hooks for side effects (logging, external telemetry). All hooks are wrapped in try/catch — errors never break the stream.

```typescript
await server.register({
    plugin: SsePlugin,
    options: {
        hooks: {
            onSession: (session, path, params) => {
                console.log(`New connection: ${path}`);
            },
            onSessionClose: (session, path, params) => {
                console.log(`Disconnected: ${path}`);
            },
            onPublish: (path, data, deliveryCount) => {
                console.log(`Published to ${path}: ${deliveryCount} recipients`);
            },
        },
    },
});
```

For metrics, prefer `server.sse.stats()` which tracks counters automatically. Use hooks for side effects like logging or pushing to external systems.

## Generics

Subscription config and publish are generic for type-safe event payloads:

```typescript
interface ChatMessage {
    text: string;
    user: string;
}

server.sse.subscription<ChatMessage>('/chat', {
    filter: (path, message) => {
        // message is typed as ChatMessage
        return message.user !== 'blocked';
    },
});

await server.sse.publish<ChatMessage>('/chat', { text: 'hi', user: 'alice' });
```

## Exports

```typescript
// Classes
export { EventBuffer, Session, SsePlugin, FiniteReplayer, ValidReplayer };

// Types
export type {
    SsePluginOptions,
    SseApi,
    SseHandlerOptions,
    SseHooks,
    SseStats,
    SubscriptionConfig,
    SubscriptionInfo,
    FilterOptions,
    BackpressureOptions,
    Replayer,
    ReplayEntry,
};
```
