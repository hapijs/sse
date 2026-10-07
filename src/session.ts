import type { Request, ResponseObject, ResponseToolkit } from '@hapi/hapi';
import { randomUUID } from 'node:crypto';
import { PassThrough } from 'node:stream';

import { EventBuffer } from './event-buffer.js';

export interface BackpressureOptions {
    maxBytes: number;
    strategy: 'close' | 'drop';
}

export interface SessionOptions {
    request: Request;
    retry: number | null;
    keepAlive: { interval: number } | false;
    headers: Record<string, string>;
    backpressure?: BackpressureOptions;
    maxDuration?: number;
}

export const readLastEventId = (request: Request): string => {
    const raw = request.headers['last-event-id'];

    return ((Array.isArray(raw) ? raw[0] : raw) ?? '').replace(/[\x00-\x1f]/g, '');
};

export class Session {
    readonly id: string = randomUUID();
    readonly request: Request;
    readonly lastEventId: string;
    readonly connectedAt: number;
    /** @internal */
    readonly #stream = new PassThrough();
    /** @internal */
    readonly #buffer: EventBuffer;
    /** @internal */
    readonly #retry: number | null;
    /** @internal */
    readonly #keepAlive: { interval: number } | false;
    /** @internal */
    readonly #headers: Record<string, string>;
    /** @internal */
    readonly #backpressure: BackpressureOptions | undefined;
    /** @internal */
    readonly #maxDuration: number | undefined;
    /** @internal */
    readonly #metadata = new Map<string, unknown>();
    /** @internal */
    #keepAliveTimer: ReturnType<typeof setInterval> | null = null;
    /** @internal */
    #maxDurationTimer: ReturnType<typeof setTimeout> | null = null;
    /** @internal */
    #closed = false;
    /** @internal */
    #initialized = false;

    constructor(options: SessionOptions) {
        this.request = options.request;
        this.connectedAt = Date.now();
        this.lastEventId = readLastEventId(options.request);
        this.#buffer = new EventBuffer();
        this.#retry = options.retry;
        this.#keepAlive = options.keepAlive;
        this.#headers = options.headers;
        this.#backpressure = options.backpressure;
        this.#maxDuration = options.maxDuration;
        this.#stream.once('close', () => this.close());
    }

    get isOpen(): boolean {
        return !this.#closed;
    }

    set(key: string, value: unknown): void {
        this.#metadata.set(key, value);
    }

    get(key: string): unknown {
        return this.#metadata.get(key);
    }

    has(key: string): boolean {
        return this.#metadata.has(key);
    }

    delete(key: string): boolean {
        return this.#metadata.delete(key);
    }

    initialize(): void {
        if (this.#closed || this.#initialized) {
            return;
        }

        this.#initialized = true;

        const socket = this.request.raw.req.socket;

        if ('setNoDelay' in socket) {
            (socket as { setNoDelay: (v: boolean) => void }).setNoDelay(true);
        }

        if (this.#retry !== null) {
            this.#buffer.retry(this.#retry);
        }

        this.#buffer.comment('ok');
        this.#buffer.dispatch();
        this.#flush();

        if (this.#keepAlive) {
            this.#keepAliveTimer = setInterval(() => this.#onKeepAlive(), this.#keepAlive.interval);
        }

        if (this.#maxDuration) {
            const jitter = this.#maxDuration * 0.1 * (2 * Math.random() - 1);

            this.#maxDurationTimer = setTimeout(() => {
                this.comment('session expired');
                this.close();
            }, this.#maxDuration + jitter);
        }
    }

    /** @internal */
    #onKeepAlive(): void {
        this.#buffer.comment();
        this.#buffer.dispatch();
        this.#flush();
    }

    push(data: unknown, event?: string, id?: string): boolean {
        if (this.#closed) {
            return false;
        }

        this.#buffer.push(data, event, id);

        if (this.#backpressure) {
            const payload = this.#buffer.read();
            const pendingBytes =
                this.#stream.readableLength +
                this.#stream.writableLength +
                this.request.raw.res.writableLength +
                Buffer.byteLength(payload, 'utf8');

            if (pendingBytes > this.#backpressure.maxBytes) {
                this.#buffer.clear();

                if (this.#backpressure.strategy === 'close') {
                    this.close();
                }

                return false;
            }
        }

        this.#flush();

        return true;
    }

    comment(text?: string): void {
        if (this.#closed) {
            return;
        }

        this.#buffer.comment(text);
        this.#buffer.dispatch();
        this.#flush();
    }

    async complete(): Promise<void> {
        if (this.#closed || !this.#initialized) {
            return;
        }

        this.#buffer.push({ complete: true }, 'complete', this.id);
        this.#flush();

        const store = this.request.route.realm.plugins['@hapi/sse']?.completionStore;

        if (store) {
            await store.set(this.id, true, 0);
        }

        this.close();
    }

    close(): void {
        if (this.#closed) {
            return;
        }

        this.#closed = true;

        if (this.#keepAliveTimer) {
            clearInterval(this.#keepAliveTimer);
            this.#keepAliveTimer = null;
        }

        if (this.#maxDurationTimer) {
            clearTimeout(this.#maxDurationTimer);
            this.#maxDurationTimer = null;
        }

        this.#stream.end();
    }

    /** @internal */
    onClose(listener: () => void): void {
        this.#stream.once('close', listener);
    }

    /** @internal */
    respond(h: ResponseToolkit): ResponseObject {
        const response = h.response(this.#stream).type('text/event-stream');

        // Without a charset hapi would append "; charset=utf-8" to every text/* type.
        response.charset();

        const headers: Record<string, string> = {
            'cache-control': 'no-cache',
            connection: 'keep-alive',
            'x-accel-buffering': 'no',
            ...this.#headers,
        };

        for (const [name, value] of Object.entries(headers)) {
            response.header(name, value);
        }

        return response;
    }

    /** @internal */
    #flush(): void {
        const data = this.#buffer.read();

        if (data) {
            this.#stream.write(data);
            this.#buffer.clear();
        }
    }
}
