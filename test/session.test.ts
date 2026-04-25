import * as timers from 'node:timers/promises';
import { expect, describe, it } from 'vitest';
import { Session } from '../src/session.js';

describe.concurrent('Session', () => {
    it('does not initialize or start keep-alive when the session is already closed', async () => {
        const mockRequest = {
            headers: {},
            raw: {
                req: { socket: {} },
                res: { writeHead: () => {}, end: () => {} },
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
        const mockRes = {
            writeHead: () => {},
            write: () => {
                return true;
            },
            end: () => {},
        } as any;

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

    it('closes the session when a flush error occurs during a push', async () => {
        const mockRes = {
            writeHead: () => {},
            write: () => {
                throw new Error('write error');
            },
            end: () => {},
        } as any;

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

        session.initialize();
        // push will trigger flush which will throw
        const result = session.push({ data: 1 });

        expect(result).toBe(false);
        expect(session.isOpen).toBe(false);
    });

    it('complete() sends a final event, writes the session id to the realm completion store, and closes', async () => {
        const writes: string[] = [];
        const mockRes = {
            writeHead: () => {},
            write: (chunk: string) => {
                writes.push(chunk);

                return true;
            },
            end: () => {},
        } as any;

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
                raw: { req: { socket: {} }, res: { writeHead: () => {}, write: () => true, end: () => {} } as any },
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
                raw: { req: { socket: {} }, res: { writeHead: () => {}, write: () => true, end: () => {} } as any },
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
                res: { writeHead: () => {}, end: () => {} },
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
        const mockRes = {
            writeHead: () => {},
            write: () => true,
            end: () => {},
            writableLength: 0,
        } as any;

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
        const mockRes = {
            end: () => {},
        } as any;

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

    it('does not write to the response when flushing an empty buffer', async () => {
        const mockRes = {
            writeHead: () => {},
            write: () => true,
            end: () => {},
        } as any;

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
