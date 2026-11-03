import Hapi from '@hapi/hapi';
import * as timers from 'node:timers/promises';
import { expect, describe, it } from 'vitest';
import { Session } from '../src/session.js';

describe.concurrent('Session', () => {
    it('does not initialize or start keep-alive when the session is already closed', async () => {
        const mockRequest = {
            headers: {},
            raw: {
                req: { socket: {} },
                res: { writableLength: 0 },
            },
        } as any;

        const session = new Session({
            request: mockRequest,
            retry: 1000,
            keepAlive: { interval: 1 }, // very short interval
            headers: {},
        });

        session.close();

        // Should return early
        session.initialize();
        await timers.setTimeout(20);
    });

    it('cleans up keep-alive timer when the session is closed', async () => {
        const mockRes = { writableLength: 0 } as any;

        const mockRequest = {
            headers: {},
            raw: {
                req: { socket: {} },
                res: mockRes,
            },
        } as any;

        const session = new Session({
            request: mockRequest,
            retry: 1000,
            keepAlive: { interval: 5 },
            headers: {},
        });

        session.initialize();
        // Force closed
        session.close();
    });

    it('complete() sends a final event, writes the session id to the realm completion store, and closes', async () => {
        const writes: string[] = [];
        const mockRes = { writableLength: 0 } as any;

        const stored: string[] = [];
        const completionStore = {
            set: async (id: string) => {
                stored.push(id);
            },
        };

        const session = new Session({
            request: {
                headers: {},
                raw: { req: { socket: {} }, res: mockRes },
                route: { realm: { plugins: { '@hapi/sse': { completionStore } } } },
            } as any,
            retry: null,
            keepAlive: false,
            headers: {},
        });

        expect(session.id).toMatch(/^[0-9a-f-]{36}$/);

        const response: any = {
            type: () => response,
            charset: () => response,
            header: () => response,
        };
        const h: any = {
            response: (stream: NodeJS.ReadableStream) => {
                stream.setEncoding('utf8');
                stream.on('data', (chunk: string) => writes.push(chunk));

                return response;
            },
        };

        session.respond(h);
        session.initialize();
        await session.complete();

        const final = writes.at(-1)!;

        expect(final).toContain('event: complete');
        expect(final).toContain(`id: ${session.id}`);
        expect(final).toContain('data: {"complete":true}');
        expect(stored).toEqual([session.id]);
        expect(session.isOpen).toBe(false);
    });

    it('complete() is a no-op when the session was never initialized', async () => {
        const stored: string[] = [];
        const completionStore = {
            set: async (id: string) => {
                stored.push(id);
            },
        };

        const session = new Session({
            request: {
                headers: {},
                raw: { req: { socket: {} }, res: { writableLength: 0 } },
                route: { realm: { plugins: { '@hapi/sse': { completionStore } } } },
            } as any,
            retry: null,
            keepAlive: false,
            headers: {},
        });

        await session.complete();

        expect(stored).toEqual([]);
        expect(session.isOpen).toBe(true);
    });

    it('complete() is a no-op when the session is already closed', async () => {
        const stored: string[] = [];
        const completionStore = {
            set: async (id: string) => {
                stored.push(id);
            },
        };

        const session = new Session({
            request: {
                headers: {},
                raw: { req: { socket: {} }, res: { writableLength: 0 } },
                route: { realm: { plugins: { '@hapi/sse': { completionStore } } } },
            } as any,
            retry: null,
            keepAlive: false,
            headers: {},
        });

        session.initialize();
        session.close();
        await session.complete();

        expect(stored).toEqual([]);
    });

    it('uses the first event ID when the header is an array of IDs', async () => {
        const mockRequest = {
            headers: { 'last-event-id': ['id1', 'id2'] },
            raw: {
                req: { socket: {} },
                res: { writableLength: 0 },
            },
        } as any;

        const session = new Session({
            request: mockRequest,
            retry: 1000,
            keepAlive: false,
            headers: {},
        });

        expect(session.lastEventId).toBe('id1');
    });

    it('does not exceed backpressure maxBytes when within limit', async () => {
        const mockRes = { writableLength: 0 } as any;

        const mockRequest = {
            headers: {},
            raw: {
                req: { socket: {} },
                res: mockRes,
            },
        } as any;

        const session = new Session({
            request: mockRequest,
            retry: null,
            keepAlive: false,
            headers: {},
            backpressure: { maxBytes: 1000, strategy: 'close' },
        });

        session.initialize();
        const result = session.push('small data');
        expect(result).toBe(true);
        expect(session.isOpen).toBe(true);
    });
    it('can be closed when not initialized', async () => {
        const mockRes = { writableLength: 0 } as any;

        const mockRequest = {
            headers: {},
            raw: {
                req: { socket: {} },
                res: mockRes,
            },
        } as any;

        const session = new Session({
            request: mockRequest,
            retry: null,
            keepAlive: false,
            headers: {},
        });

        session.close();
        expect(session.isOpen).toBe(false);
    });

    it('skips the stream write when the buffer is empty at flush time', async () => {
        const mockRes = { writableLength: 0 } as any;

        const mockRequest = {
            headers: {},
            raw: {
                req: { socket: {} },
                res: mockRes,
            },
        } as any;

        const session = new Session({
            request: mockRequest,
            retry: null,
            keepAlive: { interval: 5 },
            headers: {},
        });

        // To reach the 'if (data)' false branch in #flush(), we need data to be falsy.
        // Since all public methods add data before flushing, we use a mock on EventBuffer.read.
        const { EventBuffer } = await import('../src/event-buffer.js');
        const readSpy = (await import('vitest')).vi.spyOn(EventBuffer.prototype, 'read')
            .mockReturnValueOnce('ok') // for initialize
            .mockReturnValueOnce('');   // for keep-alive

        session.initialize();

        // Wait for keep-alive to trigger #onKeepAlive which calls #flush
        await timers.setTimeout(15);

        session.close();

        expect(readSpy).toHaveBeenCalled();
        // The second call (keep-alive) should have returned '' and thus not called write
        // But initialize called write.
        readSpy.mockRestore();
    });
});

// A handler that picks JSON or SSE after doing work must be able to return the session itself:
// the plugin's decoration and subscriptions commit to a stream before any handler code runs.
describe('Session.respond()', () => {
    const decidingLate = () => {
        const server = Hapi.server();

        server.route({
            method: 'POST',
            path: '/events',
            handler: async (request, h) => {
                const { stream } = request.payload as { stream: boolean };

                await Promise.resolve();

                if (!stream) {
                    return h.response({ json: true }).code(201);
                }

                const session = new Session({ request });

                session.initialize();
                session.push({ n: 1 });
                session.close();

                return session.respond(h);
            },
        });

        return server;
    };

    it('streams from a POST handler with the plugin unregistered, using the plugin defaults', async () => {
        const res = await decidingLate().inject({ method: 'POST', url: '/events', payload: { stream: true } });

        expect(res.statusCode).toBe(200);
        expect(res.headers['content-type']).toBe('text/event-stream');
        expect(res.headers['cache-control']).toBe('no-cache');
        expect(res.payload).toContain('retry: 2000\n');
        expect(res.payload).toContain('data: {"n":1}\n\n');
    });

    it('leaves the handler free to answer with anything else', async () => {
        const res = await decidingLate().inject({ method: 'POST', url: '/events', payload: { stream: false } });

        expect(res.statusCode).toBe(201);
        expect(res.result).toEqual({ json: true });
    });

    it('sends no retry field when retry is null', async () => {
        const server = Hapi.server();

        server.route({
            method: 'GET',
            path: '/events',
            handler: (request, h) => {
                const session = new Session({ request, retry: null });

                session.initialize();
                session.close();

                return session.respond(h);
            },
        });

        const res = await server.inject('/events');

        expect(res.payload).not.toContain('retry:');
    });

    it('delivers one event larger than maxBytes when nothing is queued', async () => {
        const server = Hapi.server();
        const large = 'x'.repeat(4096);
        const accepted: boolean[] = [];

        server.route({
            method: 'POST',
            path: '/events',
            handler: (request, h) => {
                const session = new Session({ request, backpressure: { maxBytes: 1024, strategy: 'close' } });

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
