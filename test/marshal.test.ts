import * as timers from 'node:timers/promises';
import { expect, describe, it } from 'vitest';
import http from 'node:http';
import crypto from 'node:crypto';
import zlib from 'node:zlib';

import Hapi from '@hapi/hapi';

import { SsePlugin } from '../src/sse.js';
import type { Session } from '../src/session.js';

// A browser rejects a credentialed cross-origin stream without CORS headers, which only hapi's marshal step adds.

const ORIGIN = 'http://app.test';

interface Chunk {
    at: number;
    text: string;
}

interface Received {
    status: number;
    headers: http.IncomingHttpHeaders;
    chunks: Chunk[];
}

const receive = (
    url: string,
    opts: { headers?: Record<string, string>; until: (text: string) => boolean; timeout?: number },
): Promise<Received> => {
    const { headers = {}, until, timeout = 2000 } = opts;

    return new Promise((resolve) => {
        const chunks: Chunk[] = [];
        let text = '';
        let status = 0;
        let responseHeaders: http.IncomingHttpHeaders = {};

        const done = () => {
            clearTimeout(timer);
            req.destroy();
            resolve({ status, headers: responseHeaders, chunks });
        };

        const req = http.get(url, { headers }, (res) => {
            status = res.statusCode!;
            responseHeaders = res.headers;

            const body = res.headers['content-encoding'] ? res.pipe(zlib.createUnzip()) : res;

            body.setEncoding('utf8');
            body.on('data', (chunk: string) => {
                chunks.push({ at: Date.now(), text: chunk });
                text += chunk;

                if (until(text)) {
                    done();
                }
            });

            body.on('end', done);
        });

        req.on('error', () => {});

        const timer = setTimeout(done, timeout);
    });
};

const corsServer = async () => {
    const server = Hapi.server({
        port: 0,
        routes: { cors: { origin: [ORIGIN], credentials: true }, security: true },
    });

    await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

    return server;
};

describe.concurrent('SSE response marshalling', () => {
    it('applies route CORS to a subscription stream for an allowed origin', async ({ onTestFinished }) => {
        const server = await corsServer();
        onTestFinished(() => server.stop());
        server.sse.subscription('/events');
        await server.start();

        const res = await receive(`http://localhost:${server.info.port}/events`, {
            headers: { origin: ORIGIN },
            until: (text) => text.includes(': ok'),
        });

        expect(res.status).toBe(200);
        expect(res.headers['content-type']).toBe('text/event-stream');
        expect(res.headers['access-control-allow-origin']).toBe(ORIGIN);
        expect(res.headers['access-control-allow-credentials']).toBe('true');
        expect(res.headers.vary).toContain('origin');
    });

    it('omits allow-origin for a disallowed origin', async ({ onTestFinished }) => {
        const server = await corsServer();
        onTestFinished(() => server.stop());
        server.sse.subscription('/events');
        await server.start();

        const res = await receive(`http://localhost:${server.info.port}/events`, {
            headers: { origin: 'http://evil.test' },
            until: (text) => text.includes(': ok'),
        });

        expect(res.headers['access-control-allow-origin']).toBeUndefined();
    });

    it('applies route CORS and security headers to an sse handler stream', async ({ onTestFinished }) => {
        const server = await corsServer();
        onTestFinished(() => server.stop());
        server.route({
            method: 'GET',
            path: '/stream',
            handler: { sse: { stream: (_request, session) => void session.push('hi') } },
        });
        await server.start();

        const res = await receive(`http://localhost:${server.info.port}/stream`, {
            headers: { origin: ORIGIN },
            until: (text) => text.includes('data: hi'),
        });

        expect(res.status).toBe(200);
        expect(res.headers['access-control-allow-origin']).toBe(ORIGIN);
        expect(res.headers['x-frame-options']).toBe('DENY');
        expect(res.headers['x-content-type-options']).toBe('nosniff');
    });

    it('runs onPreResponse on the stream response', async ({ onTestFinished }) => {
        const server = await corsServer();
        onTestFinished(() => server.stop());

        let seen: string | undefined;

        server.ext('onPreResponse', (request, h) => {
            const response = request.response as Hapi.ResponseObject;

            seen = response.headers['content-type'] as string;
            response.header('x-seen', 'yes');

            return h.continue;
        });

        server.sse.subscription('/events');
        await server.start();

        const res = await receive(`http://localhost:${server.info.port}/events`, {
            until: (text) => text.includes(': ok'),
        });

        expect(seen).toBe('text/event-stream');
        expect(res.headers['x-seen']).toBe('yes');
    });

    it('keeps cache-control, x-accel-buffering and the headers option on the marshalled response', async ({
        onTestFinished,
    }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.route({
            method: 'GET',
            path: '/stream',
            handler: { sse: { headers: { 'x-custom': 'abc' }, stream: () => {} } },
        });
        await server.start();

        const res = await receive(`http://localhost:${server.info.port}/stream`, {
            until: (text) => text.includes(': ok'),
        });

        expect(res.headers['cache-control']).toBe('no-cache');
        expect(res.headers['x-accel-buffering']).toBe('no');
        expect(res.headers['x-custom']).toBe('abc');
    });

    it.for(['gzip', 'deflate'])('streams %s-compressed events one at a time', async (encoding, { onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        const sentAt: number[] = [];

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: async (_request, session) => {
                        for (let i = 0; i < 3; ++i) {
                            await timers.setTimeout(150);
                            sentAt.push(Date.now());
                            session.push(`e${i}`);
                        }

                        session.close();
                    },
                },
            },
        });
        await server.start();

        const res = await receive(`http://localhost:${server.info.port}/stream`, {
            headers: { 'accept-encoding': encoding },
            until: (text) => text.includes('data: e2'),
        });

        expect(res.headers['content-encoding']).toBe(encoding);

        const firstOk = res.chunks.find((c) => c.text.includes(': ok'))!;

        expect(firstOk.at).toBeLessThan(sentAt[0]!);

        for (let i = 0; i < 2; ++i) {
            const arrival = res.chunks.find((c) => c.text.includes(`data: e${i}`))!;

            expect(arrival.at).toBeLessThan(sentAt[i + 1]!);
        }
    });

    it('delivers the last event of a gzip burst larger than the zlib buffers', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const url = `http://localhost:${server.info.port}/events`;
        const received = receive(url, {
            headers: { 'accept-encoding': 'gzip' },
            until: (text) => text.includes('data: final'),
        });

        while (server.sse.sessionCount === 0) {
            await timers.setTimeout(5);
        }

        for (let i = 0; i < 150; ++i) {
            await server.sse.publish('/events', crypto.randomBytes(512).toString('hex'));
        }

        await server.sse.publish('/events', 'final');

        const res = await received;

        expect(res.headers['content-encoding']).toBe('gzip');
        expect(res.chunks.map((c) => c.text).join('')).toContain('data: final');
    });

    it.for<[string, Hapi.ServerOptions]>([
        ['compression: false', { compression: false }],
        ['a mime override', { mime: { override: { 'text/event-stream': { compressible: false } } } }],
    ])('sends events uncompressed when the server turns compression off with %s', async ([, serverOptions], {
        onTestFinished,
    }) => {
        const server = Hapi.server({ port: 0, ...serverOptions });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });
        server.sse.subscription('/events');
        await server.start();

        const res = await receive(`http://localhost:${server.info.port}/events`, {
            headers: { 'accept-encoding': 'gzip' },
            until: (text) => text.includes(': ok'),
        });

        expect(res.status).toBe(200);
        expect(res.headers['content-encoding']).toBeUndefined();
    });

    it('sends replayed events before a slow onReconnect resolves', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        let reconnected = false;

        server.sse.subscription('/events', {
            replay: {
                record: () => {},
                replay: () => [{ id: '2', data: 'missed' }],
            },
            onReconnect: async () => {
                await timers.setTimeout(1000);
                reconnected = true;
            },
        });
        await server.start();

        const res = await receive(`http://localhost:${server.info.port}/events`, {
            headers: { 'last-event-id': '1' },
            until: (text) => text.includes('data: missed'),
        });

        expect(res.status).toBe(200);
        expect(res.chunks.map((c) => c.text).join('')).toContain('data: missed');
        expect(reconnected).toBe(false);
    });

    it('releases maxSessions slots for clients that disconnect during onSubscribe', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        let unsubscribed = 0;

        server.sse.subscription('/events', {
            maxSessions: 2,
            onSubscribe: () => timers.setTimeout(150),
            onUnsubscribe: () => {
                unsubscribed++;
            },
        });
        await server.start();

        const url = `http://localhost:${server.info.port}/events`;
        const dropEarly = () => receive(url, { until: () => false, timeout: 50 });

        await Promise.all([dropEarly(), dropEarly()]);

        while (unsubscribed < 2) {
            await timers.setTimeout(10);
        }

        const res = await receive(url, { until: (text) => text.includes(': ok') });

        expect(res.status).toBe(200);
        expect(unsubscribed).toBe(2);
    });

    it('closes the session when the client disconnects from an sse handler stream', async ({ onTestFinished }) => {
        const server = Hapi.server({ port: 0 });
        onTestFinished(() => server.stop());
        await server.register({ plugin: SsePlugin, options: { retry: null, keepAlive: false } });

        let sessionRef: Session | undefined;

        server.route({
            method: 'GET',
            path: '/stream',
            handler: {
                sse: {
                    stream: (_request, session) => {
                        sessionRef = session;
                    },
                },
            },
        });
        await server.start();

        const finished = new Promise((resolve) => server.events.once('response', resolve));

        await receive(`http://localhost:${server.info.port}/stream`, { until: (text) => text.includes(': ok') });
        await finished;

        expect(sessionRef!.isOpen).toBe(false);
    });

    it('returns 204 with CORS headers when refuse() refuses', async ({ onTestFinished }) => {
        const server = await corsServer();
        onTestFinished(() => server.stop());
        server.sse.subscription('/events', { refuse: () => true });
        await server.start();

        const res = await receive(`http://localhost:${server.info.port}/events`, {
            headers: { origin: ORIGIN },
            until: () => false,
        });

        expect(res.status).toBe(204);
        expect(res.headers['access-control-allow-origin']).toBe(ORIGIN);
    });

    it('returns 204 with CORS headers when reconnecting to a completed session', async ({ onTestFinished }) => {
        const server = await corsServer();
        onTestFinished(() => server.stop());
        server.sse.subscription('/events');
        await server.start();

        const url = `http://localhost:${server.info.port}/events`;
        const first = receive(url, { headers: { origin: ORIGIN }, until: (text) => text.includes('complete') });

        while (server.sse.sessionCount === 0) {
            await timers.setTimeout(5);
        }

        let completedId = '';

        await server.sse.eachSession(async (session) => {
            completedId = session.id;
            await session.complete();
        });
        await first;

        const res = await receive(url, {
            headers: { origin: ORIGIN, 'last-event-id': completedId },
            until: () => false,
        });

        expect(res.status).toBe(204);
        expect(res.headers['access-control-allow-origin']).toBe(ORIGIN);
    });

    it('returns 204 when onSubscribe closes the session', async ({ onTestFinished }) => {
        const server = await corsServer();
        onTestFinished(() => server.stop());
        server.sse.subscription('/events', { onSubscribe: (session) => session.close() });
        await server.start();

        const res = await receive(`http://localhost:${server.info.port}/events`, {
            headers: { origin: ORIGIN },
            until: () => false,
            timeout: 500,
        });

        expect(res.status).toBe(204);
        expect(res.headers['access-control-allow-origin']).toBe(ORIGIN);
    });
});
