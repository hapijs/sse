import { describe, expect, it } from 'vitest';

import Hapi from '@hapi/hapi';

import { Session } from '../src/session.js';

// A handler that picks JSON or SSE after doing work must be able to return the session itself:
// the plugin's decoration and subscriptions commit to a stream before any handler code runs.

const decidingLate = () => {
    const server = Hapi.server();

    server.route({
        method: 'POST',
        path: '/events',
        handler: async (request, h) => {
            const { stream } = request.payload as { stream: boolean };
            const session = new Session({ request, retry: null, keepAlive: false, headers: {} });

            await Promise.resolve();

            if (!stream) {
                session.close();

                return h.response({ json: true }).code(201);
            }

            session.initialize();
            session.push({ n: 1 });
            session.close();

            return session.respond(h);
        },
    });

    return server;
};

describe('backpressure', () => {
    it('delivers one event larger than maxBytes when nothing is queued', async () => {
        const server = Hapi.server();
        const large = 'x'.repeat(4096);
        const accepted: boolean[] = [];

        server.route({
            method: 'POST',
            path: '/events',
            handler: (request, h) => {
                const session = new Session({
                    request,
                    retry: null,
                    keepAlive: false,
                    headers: {},
                    backpressure: { maxBytes: 1024, strategy: 'close' },
                });

                session.initialize();
                accepted.push(session.push(large), session.push('queued behind the large event'));
                session.close();

                return session.respond(h);
            },
        });

        const res = await server.inject({ method: 'POST', url: '/events' });

        expect(accepted).toEqual([true, false]);
        expect(res.payload).toContain(`data: ${large}\n\n`);
    });
});

describe('session.respond()', () => {
    it('streams from a POST handler with the plugin unregistered', async () => {
        const res = await decidingLate().inject({ method: 'POST', url: '/events', payload: { stream: true } });

        expect(res.statusCode).toBe(200);
        expect(res.headers['content-type']).toBe('text/event-stream');
        expect(res.headers['cache-control']).toBe('no-cache');
        expect(res.payload).toContain('data: {"n":1}\n\n');
    });

    it('leaves the handler free to answer with anything else when it does not return the session', async () => {
        const res = await decidingLate().inject({ method: 'POST', url: '/events', payload: { stream: false } });

        expect(res.statusCode).toBe(201);
        expect(res.result).toEqual({ json: true });
    });
});
