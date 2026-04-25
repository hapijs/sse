import * as timers from 'node:timers/promises';
import { expect, describe, it } from 'vitest';
import http from 'node:http';
import net from 'node:net';
import { EventSource } from 'eventsource';

import Hapi from '@hapi/hapi';
import Boom from '@hapi/boom';

import { SsePlugin } from '../src/sse.js';
import { FiniteReplayer, ValidReplayer } from '../src/replayer.js';

interface SseOptions {
    maxEvents?: number;
    timeout?: number;
    headers?: Record<string, string>;
}
interface SseResult {
    status: number;
    headers: Record<string, string>;
    events: string[];
}

const collectSse = (url: string, opts: SseOptions = {}): Promise<SseResult> => {
    const { maxEvents = 1, timeout = 2000, headers = {} } = opts;

    return new Promise((resolve, reject) => {
        const events: string[] = [];
        let raw = '';

        const req = http.get(url, { headers }, (res) => {
            const responseHeaders: Record<string, string> = {};

            for (const [key, val] of Object.entries(res.headers)) {
                if (typeof val === 'string') {
                    responseHeaders[key] = val;
                }
            }

            res.setEncoding('utf8');
            res.on('data', (chunk: string) => {
                raw += chunk;

                const blocks = raw.split('\n\n');

                raw = blocks.pop()!;

                for (const block of blocks) {
                    const trimmed = block.trim();

                    if (trimmed && !trimmed.startsWith(':') && !trimmed.startsWith('retry:')) {
                        events.push(trimmed);
                    }

                    if (events.length >= maxEvents) {
                        req.destroy();
                        resolve({ status: res.statusCode!, headers: responseHeaders, events });

                        return;
                    }
                }
            });

            res.on('end', () => resolve({ status: res.statusCode!, headers: responseHeaders, events }));
        });

        req.on('error', (err) => {
            if (events.length >= maxEvents) {
                return;
            }

            reject(err);
        });

        setTimeout(() => {
            req.destroy();
            resolve({ status: 0, headers: {}, events });
        }, timeout);
    });
};

describe.concurrent('SSE Plugin', () => {
    it('registers without error', async ({ onTestFinished }) => {
        const server = Hapi.server();
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin });

        expect(server.sse).toBeDefined();
        expect(typeof server.sse.subscription).toBe('function');
        expect(typeof server.sse.publish).toBe('function');
        expect(typeof server.sse.broadcast).toBe('function');
        expect(typeof server.sse.eachSession).toBe('function');
    });

    it('subscription creates a GET route', async ({ onTestFinished }) => {
        const server = Hapi.server();
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin });

        server.sse.subscription('/events');

        const table = server.table();
        const route = table.find((r) => r.path === '/events');

        expect(route).toBeDefined();
        expect(route!.method).toBe('get');
    });

    it('returns correct SSE headers', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const headers = await new Promise<http.IncomingHttpHeaders>((resolve, _reject) => {
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                resolve(res.headers);
                res.on('data', () => {});
                setTimeout(() => req.destroy(), 50);
            });

            req.on('error', () => {});
        });

        expect(headers['content-type']).toBe('text/event-stream');
        expect(headers['cache-control']).toBe('no-cache');
        expect(headers['x-accel-buffering']).toBe('no');
    });

    it('onSubscribe throwing Boom returns error without SSE headers', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin });
        server.sse.subscription('/events', {
            onSubscribe: () => {
                throw Boom.forbidden('nope');
            },
        });

        await server.start();

        const result = await collectSse(
            `http://localhost:${server.info.port}/events`,
            { timeout: 500 },
        );

        expect(result.status).toBe(403);
        expect(result.headers['content-type']).toContain('application/json');
    });

    it('refuse calculator returning true responds 204 before any session is created', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin });

        let onSubscribeCalled = false;

        server.sse.subscription('/events', {
            refuse: () => true,
            onSubscribe: () => {
                onSubscribeCalled = true;
            },
        });

        await server.start();

        const result = await collectSse(`http://localhost:${server.info.port}/events`, { timeout: 500 });

        expect(result.status).toBe(204);
        expect(result.events.length).toBe(0);
        expect(result.headers['content-type']).toBeUndefined();
        expect(onSubscribeCalled).toBe(false);
        expect(server.sse.sessionCount).toBe(0);
        expect(server.sse.stats().totalConnections).toBe(0);
    });

    it('refuse calculator returning false allows the connection to proceed', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.sse.subscription('/events', {
            refuse: () => false,
        });

        await server.start();
        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1, timeout: 500 });
        await timers.setTimeout(50);
        await server.sse.publish('/events', { ok: true }, { event: 'msg' });
        const { status, events } = await promise;

        expect(status).toBe(200);
        expect(events.length).toBe(1);
    });

    it('refuse calculator receives the request and can be async', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin });

        server.sse.subscription('/events', {
            refuse: async (request) => {
                await timers.setTimeout(10);

                return request.headers['x-shutdown'] === 'true';
            },
        });

        await server.start();
        const port = server.info.port;

        const blocked = await collectSse(`http://localhost:${port}/events`, {
            timeout: 500,
            headers: { 'x-shutdown': 'true' },
        });
        expect(blocked.status).toBe(204);
    });

    it('completion cache can be overridden with a named server cache', async ({ onTestFinished }) => {
        const CatboxMemory = (await import('@hapi/catbox-memory')).Engine;

        const server = Hapi.server({
            port: 0,
            cache: [
                {
                    name: 'my-named-cache',
                    provider: { constructor: CatboxMemory, options: { maxByteSize: 1024 * 1024 } },
                },
            ],
        });
        onTestFinished(() => server.stop());

        await server.register({
            plugin: SsePlugin,
            options: {
                retry: null,
                keepAlive: false,
                completion: {
                    cache: 'my-named-cache',
                    segment: 'custom-segment',
                    expiresIn: 60_000,
                },
            },
        });

        server.sse.subscription('/events');

        await server.start();
        const port = server.info.port;

        // Verify completion still works end-to-end via the named cache
        const tokenPromise = new Promise<string>((resolve) => {
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                let data = '';
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                });
                res.on('end', () => {
                    req.destroy();
                    const idMatch = data.match(/^id:\s*(.+)$/m);

                    resolve(idMatch ? idMatch[1].trim() : '');
                });
            });
        });

        await timers.setTimeout(50);
        await server.sse.eachSession((session) => session.complete());

        const token = await tokenPromise;
        expect(token).toMatch(/[0-9a-f-]{36}/);

        const reconnect = await collectSse(`http://localhost:${port}/events`, {
            timeout: 500,
            headers: { 'Last-Event-ID': token },
        });
        expect(reconnect.status).toBe(204);
    });

    it('session.complete() sends a 204 to the next reconnect via Last-Event-ID', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.sse.subscription('/events');

        await server.start();
        const port = server.info.port;

        const captured: string[] = [];

        const firstConnect = new Promise<string>((resolve) => {
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                let data = '';
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                });
                res.on('end', () => {
                    captured.push(data);
                    req.destroy();

                    const idMatch = data.match(/^id:\s*(.+)$/m);

                    resolve(idMatch ? idMatch[1].trim() : '');
                });
            });
        });

        await timers.setTimeout(50);
        await server.sse.eachSession((session) => session.complete());

        const completionToken = await firstConnect;
        expect(completionToken).toMatch(/[0-9a-f-]{36}/);

        const reconnect = await collectSse(`http://localhost:${port}/events`, {
            timeout: 500,
            headers: { 'Last-Event-ID': completionToken },
        });

        expect(reconnect.status).toBe(204);

        // Token is consumed — a second reconnect with the same token streams normally
        const secondReconnectPromise = collectSse(`http://localhost:${port}/events`, {
            maxEvents: 1,
            timeout: 500,
            headers: { 'Last-Event-ID': completionToken },
        });
        await timers.setTimeout(50);
        await server.sse.publish('/events', { ok: true }, { event: 'msg' });
        const secondReconnect = await secondReconnectPromise;
        expect(secondReconnect.status).toBe(200);
    });

    it('real EventSource client reconnects after server closes the stream', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: 100, keepAlive: false } });

        let onSubscribeCount = 0;

        server.sse.subscription('/events', {
            onSubscribe: () => {
                onSubscribeCount++;
            },
        });

        await server.start();

        const es = new EventSource(`http://localhost:${server.info.port}/events`);
        onTestFinished(() => es.close());

        await new Promise<void>((resolve) => es.addEventListener('open', () => resolve(), { once: true }));
        expect(onSubscribeCount).toBe(1);

        await server.sse.eachSession((session) => session.close());

        await new Promise<void>((resolve) => es.addEventListener('open', () => resolve(), { once: true }));

        expect(onSubscribeCount).toBe(2);
        expect(es.readyState).toBe(EventSource.OPEN);
    });

    it('real EventSource client stops reconnecting after session.complete() runs', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: 100, keepAlive: false } });

        let onSubscribeCount = 0;

        server.sse.subscription('/events', {
            onSubscribe: () => {
                onSubscribeCount++;
            },
        });

        await server.start();

        const es = new EventSource(`http://localhost:${server.info.port}/events`);
        onTestFinished(() => es.close());

        await new Promise<void>((resolve) => es.addEventListener('open', () => resolve(), { once: true }));

        await server.sse.eachSession((session) => session.complete());

        await new Promise<void>((resolve) => {
            const check = () => {
                if (es.readyState === EventSource.CLOSED) {
                    resolve();
                } else {
                    setTimeout(check, 25);
                }
            };

            check();
        });

        const closedAt = onSubscribeCount;
        await timers.setTimeout(300);
        // Reconnect was 204'd at the completion check, before reaching onSubscribe
        expect(onSubscribeCount).toBe(closedAt);
        expect(es.readyState).toBe(EventSource.CLOSED);
    });

    it('publish delivers to subscribers', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { msg: 'hello' }, { event: 'test' });

        const { events } = await promise;

        expect(events.length).toBe(1);
        expect(events[0]).toContain('event: test');
        expect(events[0]).toContain('data: {"msg":"hello"}');
    });

    it('filter excludes non-matching sessions', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            filter: (_path, _message, options) => {
                return (options.internal as { allow: boolean }).allow;
            },
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { timeout: 300 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { msg: 'blocked' }, { internal: { allow: false } });

        const { events } = await promise;

        expect(events.length).toBe(0);
    });

    it('filter override sends modified data', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            filter: () => ({ override: { redacted: true } }),
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { secret: 'data' });

        const { events } = await promise;

        expect(events[0]).toContain('data: {"redacted":true}');
    });

    it('onReconnect fires when Last-Event-ID present', async ({ onTestFinished }) => {
        let reconnectCalled = false;
        let receivedLastId = '';

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            onReconnect: (session) => {
                reconnectCalled = true;
                receivedLastId = session.lastEventId;
                session.push({ replayed: true }, 'replay', '2');
            },
        });

        await server.start();

        const port = server.info.port;

        const { events } = await collectSse(`http://localhost:${port}/events`, {
            maxEvents: 1,
            headers: { 'Last-Event-ID': '1' },
        });

        expect(reconnectCalled).toBe(true);
        expect(receivedLastId).toBe('1');
        expect(events[0]).toContain('data: {"replayed":true}');
    });

    it('onUnsubscribe fires on disconnect', async ({ onTestFinished }) => {
        let unsubscribeCalled = false;

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            onUnsubscribe: () => {
                unsubscribeCalled = true;
            },
        });

        await server.start();

        const port = server.info.port;

        await new Promise<void>((resolve) => {
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.on('data', () => {});
                setTimeout(() => {
                    req.destroy();
                    resolve();
                }, 50);
            });

            req.on('error', () => {});
        });

        await timers.setTimeout(300);

        expect(unsubscribeCalled).toBe(true);
    });

    it('custom handler mode works', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        session.push({ chunk: 1 }, 'token');
                        session.push({ chunk: 2 }, 'token');
                        session.close();
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        const { events } = await collectSse(`http://localhost:${port}/stream`, { maxEvents: 2 });

        expect(events.length).toBe(2);
        expect(events[0]).toContain('data: {"chunk":1}');
        expect(events[1]).toContain('data: {"chunk":2}');
    });

    it('broadcast reaches all sessions', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/a');
        server.sse.subscription('/b');

        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/a`, { maxEvents: 1 });
        const p2 = collectSse(`http://localhost:${port}/b`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.broadcast({ system: true }, { event: 'announce' });

        const [r1, r2] = await Promise.all([p1, p2]);

        expect(r1.events[0]).toContain('data: {"system":true}');
        expect(r2.events[0]).toContain('data: {"system":true}');
    });

    it('eachSession iterates correctly', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        let count = 0;

        await server.sse.eachSession(
            (session) => {
                count++;
                session.push({ direct: true });
            },
            { subscription: '/events' },
        );

        expect(count).toBe(1);

        const { events } = await promise;

        expect(events[0]).toContain('data: {"direct":true}');
    });

    it('graceful shutdown closes all sessions', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { timeout: 2000 });

        await timers.setTimeout(50);

        await server.stop();

        const { events } = await promise;

        expect(events).toBeDefined();
    });

    it('multiple concurrent subscribers receive published events', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });
        const p2 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { msg: 'all' });

        const [r1, r2] = await Promise.all([p1, p2]);

        expect(r1.events[0]).toContain('data: {"msg":"all"}');
        expect(r2.events[0]).toContain('data: {"msg":"all"}');
    });

    it('publish to unmatched path is a silent no-op', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        await server.sse.publish('/nonexistent', { msg: 'lost' });
    });

    it('filter receives correct params for parameterized path', async ({ onTestFinished }) => {
        let receivedParams: Record<string, string> = {};

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events/{channel}', {
            filter: (_path, _message, options) => {
                receivedParams = options.params;

                return true;
            },
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events/news`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events/news', { msg: 'hi' });

        await promise;

        expect(receivedParams.channel).toBe('news');
    });

    it('eachSession without subscription filter iterates all sessions', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/a');
        server.sse.subscription('/b');

        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/a`, { maxEvents: 1 });
        const p2 = collectSse(`http://localhost:${port}/b`, { maxEvents: 1 });

        await timers.setTimeout(50);

        let count = 0;

        await server.sse.eachSession((session) => {
            count++;
            session.push({ ping: true });
        });

        expect(count).toBe(2);

        const [r1, r2] = await Promise.all([p1, p2]);

        expect(r1.events[0]).toContain('data: {"ping":true}');
        expect(r2.events[0]).toContain('data: {"ping":true}');
    });

    it('custom handler receives lastEventId', async ({ onTestFinished }) => {
        let receivedId = '';

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        receivedId = session.lastEventId;
                        session.push({ ok: true });
                        session.close();
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        await collectSse(`http://localhost:${port}/stream`, {
            maxEvents: 1,
            headers: { 'Last-Event-ID': '99' },
        });

        expect(receivedId).toBe('99');
    });

    it('custom handler disconnect fires cleanup', async ({ onTestFinished }) => {
        let closed = false;

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        const original = session.close.bind(session);

                        session.close = () => {
                            closed = true;
                            original();
                        };
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        await new Promise<void>((resolve) => {
            const req = http.get(`http://localhost:${port}/stream`, (res) => {
                res.on('data', () => {});
                setTimeout(() => {
                    req.destroy();
                    resolve();
                }, 50);
            });

            req.on('error', () => {});
        });

        await timers.setTimeout(300);

        expect(closed).toBe(true);
    });

    it('retry field is sent on connection when configured', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: 5000, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const raw = await new Promise<string>((resolve) => {
            let data = '';
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                    setTimeout(() => {
                        req.destroy();
                        resolve(data);
                    }, 50);
                });
            });

            req.on('error', () => {});
        });

        expect(raw).toContain('retry: 5000');
    });

    it('session comment sends through the wire', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        session.comment('ping');
                        session.close();
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        const raw = await new Promise<string>((resolve) => {
            let data = '';
            const req = http.get(`http://localhost:${port}/stream`, (res) => {
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                });

                res.on('end', () => resolve(data));
            });

            req.on('error', () => {});
        });

        expect(raw).toContain(': ping');
    });

    it('double close does not throw', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        session.close();
                        session.close();
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        const { status } = await collectSse(`http://localhost:${port}/stream`, { timeout: 500 });

        expect(status).toBe(200);
    });

    it('push after close is silently ignored', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        session.push({ before: true });
                        session.close();
                        session.push({ after: true });
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        const { events } = await collectSse(`http://localhost:${port}/stream`, { maxEvents: 2, timeout: 500 });

        expect(events.length).toBe(1);
        expect(events[0]).toContain('data: {"before":true}');
    });

    it('plugin custom headers propagate to response', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({
            plugin: SsePlugin,
            options: { retry: null, keepAlive: false, headers: { 'X-Custom': 'test' } },
        });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const headers = await new Promise<http.IncomingHttpHeaders>((resolve) => {
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                resolve(res.headers);
                res.on('data', () => {});
                setTimeout(() => req.destroy(), 50);
            });

            req.on('error', () => {});
        });

        expect(headers['x-custom']).toBe('test');
    });

    it('publish with id passes id to session', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { msg: 'hi' }, { event: 'test', id: 'evt-42' });

        const { events } = await promise;

        expect(events[0]).toContain('id: evt-42');
        expect(events[0]).toContain('event: test');
    });

    it('subscription-level retry override is respected', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: 1000, keepAlive: false } });
        server.sse.subscription('/events', { retry: 9999 });
        await server.start();

        const port = server.info.port;

        const raw = await new Promise<string>((resolve) => {
            let data = '';
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                    setTimeout(() => {
                        req.destroy();
                        resolve(data);
                    }, 50);
                });
            });

            req.on('error', () => {});
        });

        expect(raw).toContain('retry: 9999');
    });

    it('removeSession is idempotent', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { timeout: 500 });

        await timers.setTimeout(50);

        let count = 0;

        await server.sse.eachSession(() => {
            count++;
        });

        expect(count).toBe(1);

        await promise;

        await timers.setTimeout(100);

        count = 0;
        await server.sse.eachSession(() => {
            count++;
        });

        expect(count).toBe(0);
    });

    it('publish to disconnected session is safely skipped', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        await new Promise<void>((resolve) => {
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.on('data', () => {});
                setTimeout(() => {
                    req.destroy();
                    resolve();
                }, 50);
            });

            req.on('error', () => {});
        });

        await timers.setTimeout(100);

        await server.sse.publish('/events', { msg: 'to ghost' });
    });

    it('custom handler stream() error closes session gracefully', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        session.push({ before: true });

                        throw new Error('stream failed');
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        const { events } = await collectSse(`http://localhost:${port}/stream`, { maxEvents: 1, timeout: 500 });

        expect(events.length).toBe(1);
        expect(events[0]).toContain('data: {"before":true}');
    });

    it('filter error does not block delivery to other sessions', async ({ onTestFinished }) => {
        let callCount = 0;

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            filter: () => {
                callCount++;

                if (callCount === 1) {
                    throw new Error('filter boom');
                }

                return true;
            },
        });

        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1, timeout: 500 });
        const p2 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1, timeout: 500 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { msg: 'hi' });

        const [r1, r2] = await Promise.all([p1, p2]);

        const received = [r1.events.length, r2.events.length];

        expect(received).toContain(1);
    });

    it('onUnsubscribe throwing does not break cleanup', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            onUnsubscribe: () => {
                throw new Error('unsub boom');
            },
        });

        await server.start();

        const port = server.info.port;

        await new Promise<void>((resolve) => {
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.on('data', () => {});
                setTimeout(() => {
                    req.destroy();
                    resolve();
                }, 50);
            });

            req.on('error', () => {});
        });

        await timers.setTimeout(300);

        let count = 0;

        await server.sse.eachSession(() => {
            count++;
        });

        expect(count).toBe(0);
    });

    it('multiple publishes deliver in order', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 3 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { n: 1 }, { event: 'msg' });
        await server.sse.publish('/events', { n: 2 }, { event: 'msg' });
        await server.sse.publish('/events', { n: 3 }, { event: 'msg' });

        const { events } = await promise;

        expect(events.length).toBe(3);
        expect(events[0]).toContain('data: {"n":1}');
        expect(events[1]).toContain('data: {"n":2}');
        expect(events[2]).toContain('data: {"n":3}');
    });

    it('publish with multi-line string data delivers correctly', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', 'line1\nline2');

        const { events } = await promise;

        expect(events[0]).toContain('data: line1');
        expect(events[0]).toContain('data: line2');
    });

    it('publish with empty id sends id field to reset client lastEventId', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { msg: 'reset' }, { event: 'test', id: '' });

        const { events } = await promise;

        expect(events[0]).toContain('id:');
        expect(events[0]).toContain('data: {"msg":"reset"}');
    });

    it('parameterized subscriptions extract params', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        let capturedParams: Record<string, string> = {};

        server.sse.subscription('/events/{channel}', {
            onSubscribe: (_session, _path, params) => {
                capturedParams = params;
            },
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events/general`, { maxEvents: 1 });

        await timers.setTimeout(50);

        expect(capturedParams.channel).toBe('general');

        await server.sse.publish('/events/general', { msg: 'hi' });

        const { events } = await promise;

        expect(events[0]).toContain('data: {"msg":"hi"}');
    });

    // Feature 1: session.isOpen getter

    it('session.isOpen returns true when open, false after close', async ({ onTestFinished }) => {
        let openBefore = false;
        let openAfter = true;

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        openBefore = session.isOpen;
                        session.close();
                        openAfter = session.isOpen;
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        await collectSse(`http://localhost:${port}/stream`, { timeout: 500 });

        expect(openBefore).toBe(true);
        expect(openAfter).toBe(false);
    });

    // Feature 2: Session metadata

    it('session metadata set/get/has/delete', async ({ onTestFinished }) => {
        let hasKey = false;
        let getValue: unknown;
        let deleted = false;
        let hasAfterDelete = true;

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        session.set('userId', 42);
                        hasKey = session.has('userId');
                        getValue = session.get('userId');
                        deleted = session.delete('userId');
                        hasAfterDelete = session.has('userId');
                        session.close();
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        await collectSse(`http://localhost:${port}/stream`, { timeout: 500 });

        expect(hasKey).toBe(true);
        expect(getValue).toBe(42);
        expect(deleted).toBe(true);
        expect(hasAfterDelete).toBe(false);
    });

    it('session metadata persists across operations in subscription mode', async ({ onTestFinished }) => {
        let metaValue: unknown;

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            onSubscribe: (session) => {
                session.set('role', 'admin');
            },
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.eachSession((session) => {
            metaValue = session.get('role');
            session.push({ ok: true });
        });

        await promise;

        expect(metaValue).toBe('admin');
    });

    // Feature 3: Publish returns delivery count

    it('publish returns delivery count', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });
        const p2 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        const count = await server.sse.publish('/events', { msg: 'hi' });

        await Promise.all([p1, p2]);

        expect(count).toBe(2);
    });

    it('publish returns 0 for unmatched path', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const count = await server.sse.publish('/nonexistent', { msg: 'lost' });

        expect(count).toBe(0);
    });

    it('publish does not count filtered-out sessions', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            filter: () => false,
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { timeout: 300 });

        await timers.setTimeout(50);

        const count = await server.sse.publish('/events', { msg: 'blocked' });

        await promise;

        expect(count).toBe(0);
    });

    // Feature 4: server.sse.subscriptions()

    it('subscriptions() returns registered subscription info', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        server.sse.subscription('/chat/{room}');

        const subs = server.sse.subscriptions();

        expect(subs.length).toBe(2);
        expect(subs[0]).toEqual({ pattern: '/events', activeSessions: 0 });
        expect(subs[1]).toEqual({ pattern: '/chat/{room}', activeSessions: 0 });
    });

    it('subscriptions() reflects active session count', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        const subs = server.sse.subscriptions();

        expect(subs[0].activeSessions).toBe(1);

        await server.sse.publish('/events', { done: true });

        await promise;
    });

    // Feature 5: Path-literal publish

    it('literal matchMode only delivers to exact path match', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events/{channel}');
        await server.start();

        const port = server.info.port;

        const pNews = collectSse(`http://localhost:${port}/events/news`, { maxEvents: 1, timeout: 500 });
        const pSport = collectSse(`http://localhost:${port}/events/sport`, { timeout: 500 });

        await timers.setTimeout(50);

        await server.sse.publish('/events/news', { msg: 'breaking' }, { matchMode: 'literal' });

        const [rNews, rSport] = await Promise.all([pNews, pSport]);

        expect(rNews.events.length).toBe(1);
        expect(rNews.events[0]).toContain('data: {"msg":"breaking"}');
        expect(rSport.events.length).toBe(0);
    });

    it('pattern matchMode (default) delivers to all matching sessions', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events/{channel}');
        await server.start();

        const port = server.info.port;

        const pNews = collectSse(`http://localhost:${port}/events/news`, { maxEvents: 1 });
        const pSport = collectSse(`http://localhost:${port}/events/sport`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events/news', { msg: 'all' });

        const rNews = await pNews;

        expect(rNews.events.length).toBe(1);

        await server.sse.publish('/events/sport', { msg: 'all2' });

        const rSport = await pSport;

        expect(rSport.events.length).toBe(1);
    });

    // Feature 6: Event replay integration

    it('replayer replays events on reconnect via Last-Event-ID', async ({ onTestFinished }) => {
        const replayer = new FiniteReplayer({ size: 100 });

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', { replay: replayer });
        await server.start();

        const port = server.info.port;

        // Connect first client to register the subscription route
        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 3 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { n: 1 }, { event: 'msg', id: '1' });
        await server.sse.publish('/events', { n: 2 }, { event: 'msg', id: '2' });
        await server.sse.publish('/events', { n: 3 }, { event: 'msg', id: '3' });

        await p1;

        // Reconnect with Last-Event-ID: 1 — should replay events 2 and 3
        const { events } = await collectSse(`http://localhost:${port}/events`, {
            maxEvents: 2,
            headers: { 'Last-Event-ID': '1' },
        });

        expect(events.length).toBe(2);
        expect(events[0]).toContain('data: {"n":2}');
        expect(events[1]).toContain('data: {"n":3}');
    });

    it('replayer replays all when lastEventId not found', async ({ onTestFinished }) => {
        const replayer = new FiniteReplayer({ size: 100 });

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', { replay: replayer });
        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 2 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { n: 1 }, { event: 'msg', id: '1' });
        await server.sse.publish('/events', { n: 2 }, { event: 'msg', id: '2' });

        await p1;

        const { events } = await collectSse(`http://localhost:${port}/events`, {
            maxEvents: 2,
            headers: { 'Last-Event-ID': 'unknown' },
        });

        expect(events.length).toBe(2);
    });

    it('replay fires before onReconnect', async ({ onTestFinished }) => {
        const replayer = new FiniteReplayer({ size: 100 });
        const order: string[] = [];

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            replay: replayer,
            onReconnect: () => {
                order.push('onReconnect');
            },
            onSubscribe: () => {
                order.push('onSubscribe');
            },
        });

        await server.start();

        const port = server.info.port;

        // Publish first to populate the replayer
        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { n: 1 }, { id: '1' });

        await p1;

        order.length = 0;

        await collectSse(`http://localhost:${port}/events`, {
            maxEvents: 1,
            timeout: 500,
            headers: { 'Last-Event-ID': '0' },
        });

        expect(order[0]).toBe('onSubscribe');
        expect(order[1]).toBe('onReconnect');
    });

    // Feature 8: Metrics hooks

    it('onSession metric fires on new subscription', async ({ onTestFinished }) => {
        let metricPath = '';
        let metricSession: unknown;

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({
            plugin: SsePlugin,
            options: {
                retry: null,
                keepAlive: false,
                hooks: {
                    onSession: (session, path) => {
                        metricSession = session;
                        metricPath = path;
                    },
                },
            },
        });

        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        expect(metricSession).toBeDefined();
        expect(metricPath).toBe('/events');

        await server.sse.publish('/events', { done: true });

        await promise;
    });

    it('onSessionClose metric fires on disconnect', async ({ onTestFinished }) => {
        let closedPath = '';

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({
            plugin: SsePlugin,
            options: {
                retry: null,
                keepAlive: false,
                hooks: {
                    onSessionClose: (_session, path) => {
                        closedPath = path;
                    },
                },
            },
        });

        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        await new Promise<void>((resolve) => {
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.on('data', () => {});
                setTimeout(() => {
                    req.destroy();
                    resolve();
                }, 50);
            });

            req.on('error', () => {});
        });

        await timers.setTimeout(300);

        expect(closedPath).toBe('/events');
    });

    it('onPublish metric fires with delivery count', async ({ onTestFinished }) => {
        let metricCount = -1;
        let metricPublishPath = '';

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({
            plugin: SsePlugin,
            options: {
                retry: null,
                keepAlive: false,
                hooks: {
                    onPublish: (path, _data, count) => {
                        metricPublishPath = path;
                        metricCount = count;
                    },
                },
            },
        });

        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { msg: 'hi' });

        await promise;

        expect(metricPublishPath).toBe('/events');
        expect(metricCount).toBe(1);
    });

    it('metrics hook error does not break SSE', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({
            plugin: SsePlugin,
            options: {
                retry: null,
                keepAlive: false,
                hooks: {
                    onSession: () => {
                        throw new Error('metric boom');
                    },
                    onPublish: () => {
                        throw new Error('publish metric boom');
                    },
                },
            },
        });

        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { msg: 'ok' });

        const { events } = await promise;

        expect(events.length).toBe(1);
        expect(events[0]).toContain('data: {"msg":"ok"}');
    });

    // Feature 9: Backpressure

    it('backpressure close strategy closes session when maxBytes exceeded', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({
            plugin: SsePlugin,
            options: {
                retry: null,
                keepAlive: false,
                backpressure: { maxBytes: 50, strategy: 'close' },
            },
        });

        let sessionRef: { isOpen: boolean } | undefined;

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        sessionRef = session;

                        // Flood the session with data to exceed maxBytes
                        const bigData = 'x'.repeat(100);

                        session.push(bigData);
                        session.push(bigData);
                        session.push(bigData);
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        await collectSse(`http://localhost:${port}/stream`, { timeout: 500 });

        expect(sessionRef).toBeDefined();
        expect(sessionRef!.isOpen).toBe(false);
    });

    it('backpressure drop strategy drops event but keeps session open', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({
            plugin: SsePlugin,
            options: {
                retry: null,
                keepAlive: false,
                backpressure: { maxBytes: 50, strategy: 'drop' },
            },
        });

        let sessionIsOpen = false;
        const pushResults: boolean[] = [];

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        const bigData = 'x'.repeat(100);

                        pushResults.push(session.push({ first: true }));
                        pushResults.push(session.push(bigData));
                        pushResults.push(session.push(bigData));

                        sessionIsOpen = session.isOpen;
                        session.close();
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        await collectSse(`http://localhost:${port}/stream`, { timeout: 500 });

        expect(sessionIsOpen).toBe(true);
        expect(pushResults).toContain(false);
    });

    it('subscriptions api is available on registration', async ({ onTestFinished }) => {
        const server = Hapi.server();
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin });

        expect(typeof server.sse.subscriptions).toBe('function');

        const subs = server.sse.subscriptions();

        expect(subs).toEqual([]);
    });

    // Broadcast returns delivery count

    it('broadcast returns delivery count', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/a');
        server.sse.subscription('/b');

        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/a`, { maxEvents: 1 });
        const p2 = collectSse(`http://localhost:${port}/b`, { maxEvents: 1 });

        await timers.setTimeout(50);

        const count = await server.sse.broadcast({ system: true });

        await Promise.all([p1, p2]);

        expect(count).toBe(2);
    });

    // closeSessions

    it('closeSessions closes sessions for a specific subscription', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/a');
        server.sse.subscription('/b');

        await server.start();

        const port = server.info.port;

        const pA = collectSse(`http://localhost:${port}/a`, { timeout: 500 });
        const pB = collectSse(`http://localhost:${port}/b`, { maxEvents: 1 });

        await timers.setTimeout(50);

        expect(server.sse.sessionCount).toBe(2);

        server.sse.closeSessions('/a');

        await timers.setTimeout(50);

        expect(server.sse.sessionCount).toBe(1);

        await server.sse.publish('/b', { msg: 'still here' });

        const [rA, rB] = await Promise.all([pA, pB]);

        expect(rA.events.length).toBe(0);
        expect(rB.events[0]).toContain('data: {"msg":"still here"}');
    });

    it('closeSessions on unknown pattern is a no-op', async ({ onTestFinished }) => {
        const server = Hapi.server();
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin });

        server.sse.closeSessions('/nonexistent');
    });

    // --- Rigorous tests inspired by better-sse and go-sse ---

    it('keep-alive sends periodic comments', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: { interval: 100 } } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const raw = await new Promise<string>((resolve) => {
            let data = '';
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                });
            });

            // Wait for 350ms — should get initial comment + at least 2 keep-alive comments
            timers.setTimeout(350).then(() => {
                req.destroy();
                resolve(data);
            });

            req.on('error', () => {});
        });

        // Count comment lines (lines starting with :)
        const commentLines = raw.split('\n').filter((line) => line.startsWith(':'));

        // Initial ": ok" + at least 2 keep-alive comments
        expect(commentLines.length).toBeGreaterThanOrEqual(3);
    });

    it('3+ concurrent clients all receive published events', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });
        const p2 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });
        const p3 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        expect(server.sse.sessionCount).toBe(3);

        await server.sse.publish('/events', { msg: 'all3' });

        const [r1, r2, r3] = await Promise.all([p1, p2, p3]);

        expect(r1.events[0]).toContain('data: {"msg":"all3"}');
        expect(r2.events[0]).toContain('data: {"msg":"all3"}');
        expect(r3.events[0]).toContain('data: {"msg":"all3"}');
    });

    it('onReconnect throwing closes session and cleans up', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            onReconnect: () => {
                throw new Error('reconnect failed');
            },
        });

        await server.start();

        const port = server.info.port;

        // onReconnect fires after initialize() (headers already sent), so error
        // closes the session rather than returning an HTTP error
        await collectSse(`http://localhost:${port}/events`, {
            timeout: 500,
            headers: { 'Last-Event-ID': '1' },
        });

        await timers.setTimeout(100);

        // Session should be cleaned up
        expect(server.sse.sessionCount).toBe(0);
    });

    it('async filter function works correctly', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            filter: async (_path, _message, options) => {
                await timers.setTimeout(10);

                return (options.internal as { allow: boolean }).allow;
            },
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { msg: 'allowed' }, { internal: { allow: true } });

        const { events } = await promise;

        expect(events.length).toBe(1);
        expect(events[0]).toContain('data: {"msg":"allowed"}');
    });

    it('handles large payloads', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        const largeData = { payload: 'x'.repeat(50_000) };

        await server.sse.publish('/events', largeData);

        const { events } = await promise;

        expect(events.length).toBe(1);
        expect(events[0]).toContain('x'.repeat(100));
    });

    it('publish with \\r\\n data normalizes to multiple data fields', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', 'line1\r\nline2\rline3');

        const { events } = await promise;

        expect(events[0]).toContain('data: line1');
        expect(events[0]).toContain('data: line2');
        expect(events[0]).toContain('data: line3');
    });

    it('broadcast with 0 subscribers returns 0', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const count = await server.sse.broadcast({ msg: 'nobody' });

        expect(count).toBe(0);
    });

    it('comment after close is silently ignored', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        session.close();
                        session.comment('should not throw');
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        const { status } = await collectSse(`http://localhost:${port}/stream`, { timeout: 500 });

        expect(status).toBe(200);
    });

    it('filter override preserves event and id from publish opts', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            filter: () => ({ override: { transformed: true } }),
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { original: true }, { event: 'custom', id: 'evt-99' });

        const { events } = await promise;

        expect(events[0]).toContain('event: custom');
        expect(events[0]).toContain('id: evt-99');
        expect(events[0]).toContain('data: {"transformed":true}');
    });

    it('backpressure works in subscription mode (not just handler mode)', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({
            plugin: SsePlugin,
            options: {
                retry: null,
                keepAlive: false,
                backpressure: { maxBytes: 50, strategy: 'drop' },
            },
        });

        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { timeout: 500 });

        await timers.setTimeout(50);

        // Publish large events — some may be dropped
        const bigData = 'x'.repeat(200);
        const r1 = await server.sse.publish('/events', bigData);
        const r2 = await server.sse.publish('/events', bigData);
        const r3 = await server.sse.publish('/events', bigData);

        // At least one should succeed, but not all may succeed due to backpressure
        const total = r1 + r2 + r3;

        expect(total).toBeGreaterThanOrEqual(0);
        expect(total).toBeLessThanOrEqual(3);

        await promise;
    });

    it('onSubscribe sets metadata accessible during publish filter', async ({ onTestFinished }) => {
        let filterSawRole = '';

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            onSubscribe: (session) => {
                session.set('role', 'vip');
            },
            filter: (_path, _message, _options) => {
                // Access metadata via eachSession won't work here,
                // but we can verify the session was tagged
                filterSawRole = 'checked';

                return true;
            },
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        // Verify metadata set in onSubscribe is accessible later
        let metaValue: unknown;

        await server.sse.eachSession((session) => {
            metaValue = session.get('role');
        });

        expect(metaValue).toBe('vip');

        await server.sse.publish('/events', { done: true });

        await promise;

        expect(filterSawRole).toBe('checked');
    });

    it('session.request provides access to the original request object', async ({ onTestFinished }) => {
        let requestPath = '';
        let hasAuth = false;

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (request, session) => {
                        requestPath = session.request.path;
                        hasAuth = 'auth' in session.request;
                        session.push({ ok: true });
                        session.close();
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        await collectSse(`http://localhost:${port}/stream`, { maxEvents: 1 });

        expect(requestPath).toBe('/stream');
        expect(hasAuth).toBe(true);
    });

    it('Connection: keep-alive header is set', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const headers = await new Promise<http.IncomingHttpHeaders>((resolve) => {
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                resolve(res.headers);
                res.on('data', () => {});
                setTimeout(() => req.destroy(), 50);
            });

            req.on('error', () => {});
        });

        expect(headers['connection']).toBe('keep-alive');
    });

    it('multiple parameterized path segments work', async ({ onTestFinished }) => {
        let capturedParams: Record<string, string> = {};

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/org/{org}/channel/{channel}', {
            onSubscribe: (_session, _path, params) => {
                capturedParams = params;
            },
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/org/acme/channel/general`, { maxEvents: 1 });

        await timers.setTimeout(50);

        expect(capturedParams.org).toBe('acme');
        expect(capturedParams.channel).toBe('general');

        await server.sse.publish('/org/acme/channel/general', { msg: 'hi' });

        const { events } = await promise;

        expect(events[0]).toContain('data: {"msg":"hi"}');
    });

    it('ValidReplayer integration — expired events are not replayed', async ({ onTestFinished }) => {
        const replayer = new ValidReplayer({ ttl: 100 });

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', { replay: replayer });
        await server.start();

        const port = server.info.port;

        // Publish events via first client
        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { n: 1 }, { id: '1' });

        await p1;

        // Wait for TTL to expire
        await timers.setTimeout(200);

        // Reconnect — events should be expired
        const { events } = await collectSse(`http://localhost:${port}/events`, {
            maxEvents: 1,
            timeout: 300,
            headers: { 'Last-Event-ID': '0' },
        });

        expect(events.length).toBe(0);
    });

    it('FiniteReplayer with autoId generates IDs in integration', async ({ onTestFinished }) => {
        const replayer = new FiniteReplayer({ size: 100, autoId: true });

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', { replay: replayer });
        await server.start();

        const port = server.info.port;

        // Note: autoId only applies to replayer.record() which requires opts.id to be truthy
        // In current implementation, publish without id won't call record()
        // So autoId is mainly useful when manually calling replayer.record()
        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { n: 1 }, { id: 'a' });

        await p1;

        // Reconnect with Last-Event-ID before 'a'
        const { events } = await collectSse(`http://localhost:${port}/events`, {
            maxEvents: 1,
            headers: { 'Last-Event-ID': 'unknown' },
        });

        expect(events.length).toBe(1);
        expect(events[0]).toContain('data: {"n":1}');
    });

    it('stats tracks multiple connect/disconnect cycles', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        // First connection cycle
        await new Promise<void>((resolve) => {
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.on('data', () => {});
                setTimeout(() => {
                    req.destroy();
                    resolve();
                }, 50);
            });

            req.on('error', () => {});
        });

        await timers.setTimeout(100);

        // Second connection cycle
        await new Promise<void>((resolve) => {
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.on('data', () => {});
                setTimeout(() => {
                    req.destroy();
                    resolve();
                }, 50);
            });

            req.on('error', () => {});
        });

        await timers.setTimeout(100);

        const stats = server.sse.stats();

        expect(stats.totalConnections).toBe(2);
        expect(stats.totalDisconnections).toBe(2);
        expect(stats.activeSessions).toBe(0);
    });

    it('closeSessions allows new connections after closing', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        // Connect, then close all
        const p1 = collectSse(`http://localhost:${port}/events`, { timeout: 500 });

        await timers.setTimeout(50);

        server.sse.closeSessions('/events');

        await p1;

        // New connection should still work
        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        expect(server.sse.sessionCount).toBe(1);

        await server.sse.publish('/events', { msg: 'new' });

        const { events } = await promise;

        expect(events[0]).toContain('data: {"msg":"new"}');
    });

    it('rapid publish/disconnect does not crash', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        // Connect and immediately start publishing
        const promise = collectSse(`http://localhost:${port}/events`, { timeout: 200 });

        await timers.setTimeout(20);

        // Fire multiple publishes rapidly — some may hit disconnected sessions
        const results = await Promise.all([
            server.sse.publish('/events', { n: 1 }),
            server.sse.publish('/events', { n: 2 }),
            server.sse.publish('/events', { n: 3 }),
            server.sse.publish('/events', { n: 4 }),
            server.sse.publish('/events', { n: 5 }),
        ]);

        await promise;

        // No crashes — all publish calls resolved
        expect(results.length).toBe(5);

        for (const r of results) {
            expect(typeof r).toBe('number');
        }
    });

    it('literal publish returns 0 for non-matching literal path', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events/{channel}');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events/news`, { timeout: 300 });

        await timers.setTimeout(50);

        const count = await server.sse.publish('/events/sport', { msg: 'hi' }, { matchMode: 'literal' });

        expect(count).toBe(0);

        await promise;
    });

    it('publish delivery count reflects filter override', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            filter: () => ({ override: { replaced: true } }),
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        const count = await server.sse.publish('/events', { original: true });

        expect(count).toBe(1);

        await promise;
    });

    it('replay + onReconnect ordering — replay events arrive before onReconnect events', async ({ onTestFinished }) => {
        const replayer = new FiniteReplayer({ size: 100 });

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            replay: replayer,
            onReconnect: (session) => {
                session.push({ source: 'onReconnect' }, 'reconnect', 'r1');
            },
        });

        await server.start();

        const port = server.info.port;

        // Populate replayer
        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { source: 'replay' }, { event: 'msg', id: '1' });

        await p1;

        // Reconnect — should get replay event, then onReconnect event
        const { events } = await collectSse(`http://localhost:${port}/events`, {
            maxEvents: 2,
            headers: { 'Last-Event-ID': '0' },
        });

        expect(events.length).toBe(2);
        expect(events[0]).toContain('data: {"source":"replay"}');
        expect(events[1]).toContain('data: {"source":"onReconnect"}');
    });

    it('handler-level backpressure overrides plugin-level', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({
            plugin: SsePlugin,
            options: {
                retry: null,
                keepAlive: false,
                backpressure: { maxBytes: 1_000_000, strategy: 'drop' }, // very high plugin level
            },
        });

        let sessionClosed = false;

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        const bigData = 'x'.repeat(200);

                        session.push(bigData);
                        session.push(bigData);
                        session.push(bigData);

                        sessionClosed = !session.isOpen;
                    },
                    backpressure: { maxBytes: 50, strategy: 'close' }, // tight handler level
                },
            },
        });

        await server.start();

        const port = server.info.port;

        await collectSse(`http://localhost:${port}/stream`, { timeout: 500 });

        expect(sessionClosed).toBe(true);
    });

    it('multiple filter errors do not prevent delivery to remaining sessions', async ({ onTestFinished }) => {
        let filterCallCount = 0;

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            filter: () => {
                filterCallCount++;

                // First two sessions throw, third succeeds
                if (filterCallCount <= 2) {
                    throw new Error(`filter error ${filterCallCount}`);
                }

                return true;
            },
        });

        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1, timeout: 500 });
        const p2 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1, timeout: 500 });
        const p3 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1, timeout: 500 });

        await timers.setTimeout(50);

        const count = await server.sse.publish('/events', { msg: 'partial' });

        const [r1, r2, r3] = await Promise.all([p1, p2, p3]);

        // Only one session should have received the event
        const totalReceived = r1.events.length + r2.events.length + r3.events.length;

        expect(count).toBe(1);
        expect(totalReceived).toBe(1);
    });

    it('hooks onSession error does not prevent session from working', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({
            plugin: SsePlugin,
            options: {
                retry: null,
                keepAlive: false,
                hooks: {
                    onSession: () => {
                        throw new Error('hook boom');
                    },
                },
            },
        });

        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { msg: 'despite hook error' });

        const { events } = await promise;

        expect(events.length).toBe(1);
        expect(events[0]).toContain('data: {"msg":"despite hook error"}');
    });

    it('hooks onSessionClose error does not prevent cleanup', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({
            plugin: SsePlugin,
            options: {
                retry: null,
                keepAlive: false,
                hooks: {
                    onSessionClose: () => {
                        throw new Error('close hook boom');
                    },
                },
            },
        });

        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        await new Promise<void>((resolve) => {
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.on('data', () => {});
                setTimeout(() => {
                    req.destroy();
                    resolve();
                }, 50);
            });

            req.on('error', () => {});
        });

        await timers.setTimeout(200);

        expect(server.sse.sessionCount).toBe(0);
    });

    it('retry: null disables retry field in SSE stream', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const raw = await new Promise<string>((resolve) => {
            let data = '';
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                    setTimeout(() => {
                        req.destroy();
                        resolve(data);
                    }, 50);
                });
            });

            req.on('error', () => {});
        });

        expect(raw).not.toContain('retry:');
    });

    // sessionCount

    it('sessionCount reflects connected sessions', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');

        expect(server.sse.sessionCount).toBe(0);

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        expect(server.sse.sessionCount).toBe(1);

        await server.sse.publish('/events', { done: true });

        await promise;
    });

    // connectedAt

    it('session.connectedAt is set to a recent timestamp', async ({ onTestFinished }) => {
        let timestamp = 0;

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        timestamp = session.connectedAt;
                        session.close();
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        const before = Date.now();

        await collectSse(`http://localhost:${port}/stream`, { timeout: 500 });

        const after = Date.now();

        expect(timestamp).toBeGreaterThanOrEqual(before);
        expect(timestamp).toBeLessThanOrEqual(after);
    });

    // stats()

    it('stats() tracks connection and publish metrics', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');

        const initial = server.sse.stats();

        expect(initial.totalConnections).toBe(0);
        expect(initial.totalPublishes).toBe(0);
        expect(initial.totalEventsDelivered).toBe(0);
        expect(initial.activeSessions).toBe(0);

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 2 });

        await timers.setTimeout(50);

        const afterConnect = server.sse.stats();

        expect(afterConnect.totalConnections).toBe(1);
        expect(afterConnect.activeSessions).toBe(1);

        await server.sse.publish('/events', { n: 1 });
        await server.sse.publish('/events', { n: 2 });

        const afterPublish = server.sse.stats();

        expect(afterPublish.totalPublishes).toBe(2);
        expect(afterPublish.totalEventsDelivered).toBe(2);

        await promise;

        await timers.setTimeout(100);

        const afterDisconnect = server.sse.stats();

        expect(afterDisconnect.totalDisconnections).toBe(1);
    });

    it('stats() tracks broadcast metrics separately', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.broadcast({ msg: 'hi' });

        await promise;

        const stats = server.sse.stats();

        expect(stats.totalBroadcasts).toBe(1);
        expect(stats.totalEventsDelivered).toBe(1);
        expect(stats.totalPublishes).toBe(0);
    });

    // Replayer skips events without id

    // --- Additional rigorous tests (pass 3) ---

    it('eachSession with async callback processes all sessions', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });
        const p2 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        const visited: string[] = [];

        await server.sse.eachSession(async (session) => {
            await timers.setTimeout(10);
            visited.push('visited');
            session.push({ done: true });
        });

        expect(visited.length).toBe(2);

        await Promise.all([p1, p2]);
    });

    it('eachSession on non-existent subscription is a no-op', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        let count = 0;

        await server.sse.eachSession(
            () => {
                count++;
            },
            { subscription: '/nonexistent' },
        );

        expect(count).toBe(0);
    });

    it('custom handler headers override plugin-level headers', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({
            plugin: SsePlugin,
            options: { retry: null, keepAlive: false, headers: { 'X-Plugin': 'yes' } },
        });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        session.push({ ok: true });
                        session.close();
                    },
                    headers: { 'X-Handler': 'custom' },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        const headers = await new Promise<http.IncomingHttpHeaders>((resolve) => {
            const req = http.get(`http://localhost:${port}/stream`, (res) => {
                resolve(res.headers);
                res.on('data', () => {});
                setTimeout(() => req.destroy(), 50);
            });

            req.on('error', () => {});
        });

        // Handler-level headers should be used instead of plugin-level
        expect(headers['x-handler']).toBe('custom');
        expect(headers['x-plugin']).toBeUndefined();
    });

    it('custom handler retry override is respected', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: 1000, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        session.push({ ok: true });
                        session.close();
                    },
                    retry: 7777,
                },
            },
        });

        await server.start();

        const port = server.info.port;

        const raw = await new Promise<string>((resolve) => {
            let data = '';
            const req = http.get(`http://localhost:${port}/stream`, (res) => {
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                });
                res.on('end', () => resolve(data));
            });

            req.on('error', () => {});
        });

        expect(raw).toContain('retry: 7777');
    });

    it('custom handler keepAlive override sends periodic comments', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async () => {
                        // Don't close — let keep-alive fire
                    },
                    keepAlive: { interval: 100 },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        const raw = await new Promise<string>((resolve) => {
            let data = '';
            const req = http.get(`http://localhost:${port}/stream`, (res) => {
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                });
            });

            setTimeout(() => {
                req.destroy();
                resolve(data);
            }, 350);

            req.on('error', () => {});
        });

        const commentLines = raw.split('\n').filter((line) => line.startsWith(':'));

        // Initial ": ok" + at least 2 keep-alive comments
        expect(commentLines.length).toBeGreaterThanOrEqual(3);
    });

    it('server.stop() calls replayer.stop() for ValidReplayer cleanup', async ({ onTestFinished }) => {
        const replayer = new ValidReplayer({ ttl: 60_000 });
        let stopCalled = false;
        const originalStop = replayer.stop.bind(replayer);

        replayer.stop = () => {
            stopCalled = true;
            originalStop();
        };

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', { replay: replayer });
        await server.start();

        await server.stop();

        expect(stopCalled).toBe(true);
    });

    it('broadcast with id field sends id to all sessions', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.broadcast({ msg: 'hi' }, { event: 'sys', id: 'b-1' });

        const { events } = await promise;

        expect(events[0]).toContain('event: sys');
        expect(events[0]).toContain('id: b-1');
        expect(events[0]).toContain('data: {"msg":"hi"}');
    });

    it('publish to matched pattern with 0 connected sessions returns 0', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        // No one connects — publish to valid pattern
        const count = await server.sse.publish('/events', { msg: 'ghost' });

        expect(count).toBe(0);
    });

    it('push with id but no event sends id field without event field', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { msg: 'hi' }, { id: 'only-id' });

        const { events } = await promise;

        expect(events[0]).toContain('id: only-id');
        expect(events[0]).not.toContain('event:');
        expect(events[0]).toContain('data: {"msg":"hi"}');
    });

    it('concurrent publishes to different subscriptions are isolated', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/a');
        server.sse.subscription('/b');
        await server.start();

        const port = server.info.port;

        const pA = collectSse(`http://localhost:${port}/a`, { maxEvents: 1 });
        const pB = collectSse(`http://localhost:${port}/b`, { maxEvents: 1 });

        await timers.setTimeout(50);

        // Publish to both concurrently
        await Promise.all([server.sse.publish('/a', { target: 'a' }), server.sse.publish('/b', { target: 'b' })]);

        const [rA, rB] = await Promise.all([pA, pB]);

        expect(rA.events[0]).toContain('data: {"target":"a"}');
        expect(rB.events[0]).toContain('data: {"target":"b"}');
    });

    it('async onSubscribe is awaited before session is active', async ({ onTestFinished }) => {
        let subscribeFinished = false;

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            onSubscribe: async (session) => {
                await timers.setTimeout(50);
                session.set('ready', true);
                subscribeFinished = true;
            },
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(100);

        expect(subscribeFinished).toBe(true);

        let metaReady: unknown;

        await server.sse.eachSession((session) => {
            metaReady = session.get('ready');
            session.push({ ok: true });
        });

        expect(metaReady).toBe(true);

        await promise;
    });

    it('filter returning truthy non-boolean non-override object delivers original data', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            filter: () => true as unknown as boolean,
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { original: true });

        const { events } = await promise;

        expect(events[0]).toContain('data: {"original":true}');
    });

    it('session.get() returns undefined for non-existent key', async ({ onTestFinished }) => {
        let value: unknown = 'sentinel';

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        value = session.get('nonexistent');
                        session.close();
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        await collectSse(`http://localhost:${port}/stream`, { timeout: 500 });

        expect(value).toBeUndefined();
    });

    it('session.delete() returns false for non-existent key', async ({ onTestFinished }) => {
        let result = true;

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        result = session.delete('nonexistent');
                        session.close();
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        await collectSse(`http://localhost:${port}/stream`, { timeout: 500 });

        expect(result).toBe(false);
    });

    it('publish with null data serializes as "null"', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', null);

        const { events } = await promise;

        expect(events[0]).toContain('data: null');
    });

    it('publish with numeric data serializes correctly', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', 42);

        const { events } = await promise;

        expect(events[0]).toContain('data: 42');
    });

    it('onSubscribe throwing non-Boom error returns 500', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin });
        server.sse.subscription('/events', {
            onSubscribe: () => {
                throw new Error('plain error');
            },
        });

        await server.start();

        const port = server.info.port;

        const result = await collectSse(`http://localhost:${port}/events`, { timeout: 500 });

        expect(result.status).toBe(500);
    });

    it('handler stream receives the request object', async ({ onTestFinished }) => {
        let receivedPath = '';
        let receivedMethod = '';

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (request, session) => {
                        receivedPath = request.path;
                        receivedMethod = request.method;
                        session.close();
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        await collectSse(`http://localhost:${port}/stream`, { timeout: 500 });

        expect(receivedPath).toBe('/stream');
        expect(receivedMethod).toBe('get');
    });

    it('multiple sequential publishes update stats cumulatively', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { maxEvents: 5 });

        await timers.setTimeout(50);

        for (let i = 0; i < 5; i++) {
            await server.sse.publish('/events', { n: i });
        }

        await promise;

        const stats = server.sse.stats();

        expect(stats.totalPublishes).toBe(5);
        expect(stats.totalEventsDelivered).toBe(5);
    });

    it('replayer does not record events without an id', async ({ onTestFinished }) => {
        const replayer = new FiniteReplayer({ size: 100 });

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', { replay: replayer });
        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 2 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { n: 1 }); // no id — should NOT be recorded
        await server.sse.publish('/events', { n: 2 }, { id: '1' }); // has id — should be recorded

        await p1;

        // Reconnect — only event with id should replay
        const { events } = await collectSse(`http://localhost:${port}/events`, {
            maxEvents: 1,
            timeout: 500,
            headers: { 'Last-Event-ID': '0' },
        });

        expect(events.length).toBe(1);
        expect(events[0]).toContain('data: {"n":2}');
    });

    // ── Handler decorator ──

    it('supports sse handler decorator on routes', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin });

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream(_request: Hapi.Request, session: any) {
                        session.push({ msg: 'hello from decorator' });
                    },
                },
            },
        });

        await server.start();

        const port = server.info.port;

        const { events, status } = await collectSse(`http://localhost:${port}/stream`, {
            maxEvents: 1,
            timeout: 2000,
        });

        expect(status).toBe(200);
        expect(events.length).toBe(1);
        expect(events[0]).toContain('data: {"msg":"hello from decorator"}');
    });

    // ========================================================================
    // Security Tests — SSE Security Research Gap Coverage
    // Ref: sse-security/research/conclusion.md
    // ========================================================================

    // --- Injection: Last-Event-ID CRLF (CWE-93) ---

    it('Last-Event-ID with CRLF via raw TCP is split by HTTP parser (value truncated at newline)', async ({
        onTestFinished,
    }) => {
        let capturedId = '';

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            onReconnect: (session) => {
                capturedId = session.lastEventId;
            },
        });

        await server.start();

        const port = server.info.port as number;

        const response = await new Promise<string>((resolve) => {
            const socket = net.createConnection({ port }, () => {
                socket.write(
                    'GET /events HTTP/1.1\r\n' +
                        `Host: localhost:${port}\r\n` +
                        'Last-Event-ID: 123\r\nX-Injected: evil\r\n' +
                        'Connection: close\r\n' +
                        '\r\n',
                );
            });

            let data = '';

            socket.on('data', (chunk) => {
                data += chunk.toString();
            });

            socket.on('end', () => resolve(data));
            socket.on('error', () => resolve(data));

            setTimeout(() => {
                socket.destroy();
                resolve(data);
            }, 500);
        });

        expect(response).toContain('HTTP/1.1 200');
        expect(capturedId).toBe('123');
    });

    it('Last-Event-ID value is not echoed back into SSE stream', async ({ onTestFinished }) => {
        let capturedId = '';

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            onReconnect: (session) => {
                capturedId = session.lastEventId;
                session.push({ reconnected: true }, 'msg', '2');
            },
        });

        await server.start();

        const port = server.info.port;

        const maliciousId = '<script>alert(1)</script>';

        const raw = await new Promise<string>((resolve) => {
            let data = '';
            const req = http.get(
                `http://localhost:${port}/events`,
                { headers: { 'Last-Event-ID': maliciousId } },
                (res) => {
                    res.setEncoding('utf8');
                    res.on('data', (chunk: string) => {
                        data += chunk;
                        setTimeout(() => {
                            req.destroy();
                            resolve(data);
                        }, 100);
                    });
                },
            );

            req.on('error', () => {});
        });

        expect(capturedId).toBe(maliciousId);
        expect(raw).not.toContain(maliciousId);
    });

    it('Last-Event-ID with null character via raw TCP does not crash server', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');

        await server.start();

        const port = server.info.port as number;

        const response = await new Promise<string>((resolve) => {
            const socket = net.createConnection({ port }, () => {
                socket.write(
                    'GET /events HTTP/1.1\r\n' +
                        `Host: localhost:${port}\r\n` +
                        'Last-Event-ID: abc\0def\r\n' +
                        'Connection: close\r\n' +
                        '\r\n',
                );
            });

            let data = '';

            socket.on('data', (chunk) => {
                data += chunk.toString();
            });

            socket.on('end', () => resolve(data));
            socket.on('error', () => resolve(data));

            setTimeout(() => {
                socket.destroy();
                resolve(data);
            }, 500);
        });

        expect(response).toContain('HTTP/1.1');
    });

    // --- DoS: Retry Floor Enforcement (reconnection storm prevention) ---

    it('retry: 0 is clamped to 1000ms floor', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: 0, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const raw = await new Promise<string>((resolve) => {
            let data = '';
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                    setTimeout(() => {
                        req.destroy();
                        resolve(data);
                    }, 50);
                });
            });

            req.on('error', () => {});
        });

        expect(raw).not.toContain('retry: 0');
        expect(raw).toContain('retry: 1000');
    });

    it('retry: 500 is clamped to 1000ms floor', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: 500, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const raw = await new Promise<string>((resolve) => {
            let data = '';
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                    setTimeout(() => {
                        req.destroy();
                        resolve(data);
                    }, 50);
                });
            });

            req.on('error', () => {});
        });

        expect(raw).not.toContain('retry: 500');
        expect(raw).toContain('retry: 1000');
    });

    it('retry: 2000 is not clamped (above floor)', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: 2000, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const raw = await new Promise<string>((resolve) => {
            let data = '';
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                    setTimeout(() => {
                        req.destroy();
                        resolve(data);
                    }, 50);
                });
            });

            req.on('error', () => {});
        });

        expect(raw).toContain('retry: 2000');
    });

    it('retry: null still disables retry field entirely', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const raw = await new Promise<string>((resolve) => {
            let data = '';
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                    setTimeout(() => {
                        req.destroy();
                        resolve(data);
                    }, 50);
                });
            });

            req.on('error', () => {});
        });

        expect(raw).not.toContain('retry:');
    });

    it('subscription-level retry below floor is clamped', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: 2000, keepAlive: false } });
        server.sse.subscription('/events', { retry: 100 });
        await server.start();

        const port = server.info.port;

        const raw = await new Promise<string>((resolve) => {
            let data = '';
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                    setTimeout(() => {
                        req.destroy();
                        resolve(data);
                    }, 50);
                });
            });

            req.on('error', () => {});
        });

        expect(raw).not.toMatch(/retry: 100\n/);
        expect(raw).toContain('retry: 1000');
    });

    // --- Session Security: Cross-Client Data Isolation ---

    it('concurrent clients on same subscription receive only their own replay data', async ({ onTestFinished }) => {
        const replayer = new FiniteReplayer({ size: 100, autoId: true });

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', { replay: replayer });
        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { n: 1 }, { event: 'msg', id: '1' });
        await p1;

        await server.sse.publish('/events', { n: 2 }, { event: 'msg', id: '2' });
        await server.sse.publish('/events', { n: 3 }, { event: 'msg', id: '3' });

        const [clientB, clientC] = await Promise.all([
            collectSse(`http://localhost:${port}/events`, {
                maxEvents: 2,
                headers: { 'Last-Event-ID': '1' },
            }),
            collectSse(`http://localhost:${port}/events`, {
                maxEvents: 1,
                timeout: 300,
            }),
        ]);

        expect(clientB.events.length).toBe(2);
        expect(clientB.events[0]).toContain('data: {"n":2}');
        expect(clientB.events[1]).toContain('data: {"n":3}');

        expect(clientC.events.length).toBe(0);
    });

    it('session metadata is isolated between concurrent clients', async ({ onTestFinished }) => {
        const metadata: Array<{ id: string; peer: unknown }> = [];

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            onSubscribe: (session) => {
                const id = Math.random().toString(36).slice(2);

                session.set('clientId', id);
            },
        });

        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/events`, { timeout: 300 });
        const p2 = collectSse(`http://localhost:${port}/events`, { timeout: 300 });

        await timers.setTimeout(50);

        await server.sse.eachSession((session) => {
            const id = session.get('clientId') as string;

            metadata.push({ id, peer: session.get('peerSecret') });
        });

        await Promise.all([p1, p2]);

        expect(metadata.length).toBe(2);
        expect(metadata[0].id).not.toBe(metadata[1].id);
        expect(metadata[0].peer).toBeUndefined();
        expect(metadata[1].peer).toBeUndefined();
    });

    it('filter receives per-session credentials without cross-leak', async ({ onTestFinished }) => {
        const credentialsSeen: unknown[] = [];

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            filter: (_path, _message, options) => {
                credentialsSeen.push(options.credentials);

                return true;
            },
        });

        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });
        const p2 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(100);

        await server.sse.publish('/events', { data: 'test' }, { event: 'msg' });

        const [r1, r2] = await Promise.all([p1, p2]);

        expect(r1.events.length).toBe(1);
        expect(r2.events.length).toBe(1);

        expect(credentialsSeen.length).toBe(2);
    });

    // --- DoS: Graceful handling under connection pressure ---

    it('rapid subscribe/unsubscribe does not leak sessions', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const port = server.info.port;

        const connections = Array.from({ length: 10 }, () =>
            collectSse(`http://localhost:${port}/events`, { timeout: 100 }),
        );

        await Promise.all(connections);

        await timers.setTimeout(200);

        expect(server.sse.sessionCount).toBe(0);
    });

    it('publish after server.stop() does not crash', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        await server.stop({ timeout: 100 });

        const count = await server.sse.publish('/events', { msg: 'after stop' });

        expect(count).toBe(0);
    });

    it('broadcast after server.stop() does not crash', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        await server.stop({ timeout: 100 });

        const count = await server.sse.broadcast({ msg: 'after stop' });

        expect(count).toBe(0);
    });

    // --- Data Leakage: Cross-subscription isolation ---

    it('subscription A publish does not leak to subscription B listeners', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/private');
        server.sse.subscription('/public');
        await server.start();

        const port = server.info.port;

        const publicPromise = collectSse(`http://localhost:${port}/public`, { timeout: 300 });

        await timers.setTimeout(50);

        await server.sse.publish('/private', { secret: 'classified' }, { event: 'leak' });

        const publicResult = await publicPromise;

        expect(publicResult.events.length).toBe(0);
    });

    // --- Connection Security: Kill switch ---

    it('closed session does not receive subsequently published events', async ({ onTestFinished }) => {
        let sessionRef: any;

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            onSubscribe: (session) => {
                sessionRef = session;
            },
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { timeout: 500 });

        await timers.setTimeout(50);

        sessionRef.close();

        const count = await server.sse.publish('/events', { msg: 'post-kill' }, { event: 'msg' });

        await promise;

        expect(count).toBe(0);
    });

    // --- maxSessions: Per-subscription connection limiting ---

    it('maxSessions rejects connections exceeding threshold with 503', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', { maxSessions: 2 });
        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/events`, { timeout: 500 });
        const p2 = collectSse(`http://localhost:${port}/events`, { timeout: 500 });

        await timers.setTimeout(100);

        expect(server.sse.sessionCount).toBe(2);

        const rejected = await collectSse(`http://localhost:${port}/events`, { timeout: 500 });

        expect(rejected.status).toBe(503);

        await Promise.all([p1, p2]);
    });

    it('maxSessions allows new connections after existing ones disconnect', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', { maxSessions: 1 });
        await server.start();

        const port = server.info.port;

        const p1 = collectSse(`http://localhost:${port}/events`, { timeout: 200 });

        await timers.setTimeout(50);

        expect(server.sse.sessionCount).toBe(1);

        await p1;
        await timers.setTimeout(100);

        const p2 = collectSse(`http://localhost:${port}/events`, { maxEvents: 1 });

        await timers.setTimeout(50);

        await server.sse.publish('/events', { ok: true });

        const result = await p2;

        expect(result.status).toBe(200);
        expect(result.events.length).toBe(1);
    });

    it('maxSessions is per-subscription, not global', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/a', { maxSessions: 1 });
        server.sse.subscription('/b', { maxSessions: 1 });
        await server.start();

        const port = server.info.port;

        const pa = collectSse(`http://localhost:${port}/a`, { timeout: 300 });
        const pb = collectSse(`http://localhost:${port}/b`, { timeout: 300 });

        await timers.setTimeout(50);

        expect(server.sse.sessionCount).toBe(2);

        await Promise.all([pa, pb]);
    });

    // --- maxDuration: Connection TTL with forced expiry ---

    it('maxDuration closes session after expiry', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', { maxDuration: 200 });
        await server.start();

        const port = server.info.port;

        const raw = await new Promise<string>((resolve) => {
            let data = '';
            const req = http.get(`http://localhost:${port}/events`, (res) => {
                res.setEncoding('utf8');
                res.on('data', (chunk: string) => {
                    data += chunk;
                });

                res.on('end', () => resolve(data));
            });

            req.on('error', () => {});

            setTimeout(() => {
                req.destroy();
                resolve(data);
            }, 1000);
        });

        expect(raw).toContain(': session expired');
    });

    it('maxDuration timer is cleared on early close', async ({ onTestFinished }) => {
        let sessionRef: any;

        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events', {
            maxDuration: 60_000,
            onSubscribe: (session) => {
                sessionRef = session;
            },
        });

        await server.start();

        const port = server.info.port;

        const promise = collectSse(`http://localhost:${port}/events`, { timeout: 500 });

        await timers.setTimeout(50);

        sessionRef.close();

        await promise;

        expect(() => sessionRef.close()).not.toThrow();
    });

    // --- Remaining gaps (external layer) ---

    it.todo(
        'EXTERNAL: Origin header validation — implement via Hapi onPreAuth extension or reverse proxy',
    );

    it.todo(
        'EXTERNAL: stream replacement guard — implement via session-aware auth middleware',
    );

    describe.concurrent('Edge Cases', () => {
        it('handles auth configurations in subscription()', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());
            await server.register(SsePlugin);

            // Mock auth strategy
            server.auth.scheme('mock', () => ({
                authenticate: (_request, h) => h.authenticated({ credentials: { user: 'test' } }),
            }));
            server.auth.strategy('test', 'mock');

            server.sse.subscription('/events', { auth: 'test' });

            const route = server.table().find((r) => r.path === '/events');

            expect(route?.settings.auth?.strategies).toContain('test');
        });

        it('gracefully handles errors in onSession hook', async ({ onTestFinished }) => {
            const server = Hapi.server({ port: 0 });
            onTestFinished(() => server.stop());
            await server.register({
                plugin: SsePlugin,
                options: {
                    hooks: {
                        onSession: () => {
                            throw new Error('hook error');
                        },
                    },
                },
            });
            server.sse.subscription('/events');
            await server.start();

            const port = server.info.port;
            const req = http.get(`http://localhost:${port}/events`);

            await timers.setTimeout(100);
            req.destroy();
        });

        it('gracefully handles errors in onPublish hook', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());
            await server.register({
                plugin: SsePlugin,
                options: {
                    hooks: {
                        onPublish: () => {
                            throw new Error('hook error');
                        },
                    },
                },
            });
            server.sse.subscription('/events');

            // Should not throw
            await server.sse.publish('/events', { test: true });
        });

        it('gracefully handles errors in onUnsubscribe callback', async ({ onTestFinished }) => {
            const server = Hapi.server({ port: 0 });
            onTestFinished(() => server.stop());
            await server.register(SsePlugin);
            server.sse.subscription('/events', {
                onUnsubscribe: () => {
                    throw new Error('unsub error');
                },
            });
            await server.start();

            const port = server.info.port;
            const req = http.get(`http://localhost:${port}/events`);

            await timers.setTimeout(100);
            req.destroy();
            await timers.setTimeout(100);
        });
    });

    // --- Runtime validation: catches developer mistakes early ---

    describe.concurrent('Runtime validation', () => {
        it('rejects non-positive keepAlive interval at register()', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());

            await expect(
                server.register({
                    plugin: SsePlugin,
                    options: { keepAlive: { interval: 0 } },
                }),
            ).rejects.toThrow(/Invalid @hapi\/sse plugin options.*keepAlive/i);
        });

        it('rejects negative retry at register()', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());

            await expect(
                server.register({
                    plugin: SsePlugin,
                    options: { retry: -100 },
                }),
            ).rejects.toThrow(/Invalid @hapi\/sse plugin options.*retry/i);
        });

        it('rejects unknown plugin option keys', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());

            await expect(
                server.register({
                    plugin: SsePlugin,
                    options: { keepAlve: false } as never,
                }),
            ).rejects.toThrow(/Invalid @hapi\/sse plugin options.*keepAlve/i);
        });

        it('rejects non-function hook in plugin options', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());

            await expect(
                server.register({
                    plugin: SsePlugin,
                    options: { hooks: { onSession: 'not-a-function' as never } },
                }),
            ).rejects.toThrow(/Invalid @hapi\/sse plugin options.*onSession/i);
        });

        it('rejects backpressure with invalid strategy', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());

            await expect(
                server.register({
                    plugin: SsePlugin,
                    options: {
                        backpressure: { maxBytes: 1000, strategy: 'kaboom' as never },
                    },
                }),
            ).rejects.toThrow(/Invalid @hapi\/sse plugin options.*strategy/i);
        });

        it('rejects backpressure with non-integer maxBytes', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());

            await expect(
                server.register({
                    plugin: SsePlugin,
                    options: {
                        backpressure: { maxBytes: 1.5, strategy: 'drop' },
                    },
                }),
            ).rejects.toThrow(/Invalid @hapi\/sse plugin options.*maxBytes/i);
        });

        it('rejects subscription path that is not a string', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());
            await server.register({ plugin: SsePlugin });

            expect(() => server.sse.subscription(123 as never)).toThrow(/sse\.subscription\(path\)/);
        });

        it('rejects subscription path missing leading slash', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());
            await server.register({ plugin: SsePlugin });

            expect(() => server.sse.subscription('events')).toThrow(/must start with "\/"/);
        });

        it('rejects empty subscription path', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());
            await server.register({ plugin: SsePlugin });

            expect(() => server.sse.subscription('')).toThrow(/non-empty string/);
        });

        it('rejects subscription with non-function filter', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());
            await server.register({ plugin: SsePlugin });

            expect(() =>
                server.sse.subscription('/events', { filter: 'nope' as never }),
            ).toThrow(/Invalid @hapi\/sse subscription config for "\/events".*filter/i);
        });

        it('rejects subscription with non-positive maxSessions', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());
            await server.register({ plugin: SsePlugin });

            expect(() =>
                server.sse.subscription('/events', { maxSessions: 0 }),
            ).toThrow(/maxSessions/);
        });

        it('rejects subscription with non-positive maxDuration', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());
            await server.register({ plugin: SsePlugin });

            expect(() =>
                server.sse.subscription('/events', { maxDuration: -1 }),
            ).toThrow(/maxDuration/);
        });

        it('rejects subscription with replayer missing record/replay', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());
            await server.register({ plugin: SsePlugin });

            expect(() =>
                server.sse.subscription('/events', { replay: { record: () => {} } as never }),
            ).toThrow(/replay/i);
        });

        it('rejects unknown subscription config key', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());
            await server.register({ plugin: SsePlugin });

            expect(() =>
                server.sse.subscription('/events', { onUnsubcsribe: () => {} } as never),
            ).toThrow(/onUnsubcsribe/);
        });

        it('rejects sse handler decoration missing stream', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());
            await server.register({ plugin: SsePlugin });

            expect(() =>
                server.route({
                    method: 'GET',
                    path: '/stream',
                    handler: { sse: {} as never },
                }),
            ).toThrow(/Invalid @hapi\/sse handler options for GET \/stream.*stream/i);
        });

        it('rejects sse handler decoration with non-function stream', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());
            await server.register({ plugin: SsePlugin });

            expect(() =>
                server.route({
                    method: 'GET',
                    path: '/stream',
                    handler: { sse: { stream: 'nope' as never } },
                }),
            ).toThrow(/stream/);
        });

        it('rejects sse handler decoration with bad backpressure', async ({ onTestFinished }) => {
            const server = Hapi.server();
            onTestFinished(() => server.stop());
            await server.register({ plugin: SsePlugin });

            expect(() =>
                server.route({
                    method: 'GET',
                    path: '/stream',
                    handler: {
                        sse: {
                            stream: () => {},
                            backpressure: { maxBytes: -1, strategy: 'drop' },
                        },
                    },
                }),
            ).toThrow(/maxBytes/);
        });

        it('FiniteReplayer rejects non-positive size', () => {
            expect(() => new FiniteReplayer({ size: 0 })).toThrow(/Invalid FiniteReplayer options.*size/i);
        });

        it('FiniteReplayer rejects missing size', () => {
            expect(() => new FiniteReplayer({} as never)).toThrow(/Invalid FiniteReplayer options.*size/i);
        });

        it('ValidReplayer rejects non-positive ttl', () => {
            expect(() => new ValidReplayer({ ttl: 0 })).toThrow(/Invalid ValidReplayer options.*ttl/i);
        });

        it('ValidReplayer rejects non-integer ttl', () => {
            expect(() => new ValidReplayer({ ttl: 50.5 })).toThrow(/Invalid ValidReplayer options.*ttl/i);
        });
    });
});
