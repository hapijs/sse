import type { Request } from '@hapi/hapi';
import type { ServerResponse } from 'node:http';

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

export class Session {
    readonly request: Request;
    readonly lastEventId: string;
    readonly connectedAt: number;
    /** @internal */
    readonly #res: ServerResponse;
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

    constructor(options: SessionOptions) {
        this.request = options.request;
        this.connectedAt = Date.now();

        const rawId = options.request.headers['last-event-id'];

        this.lastEventId = ((Array.isArray(rawId) ? rawId[0] : rawId) ?? '').replace(/[\x00-\x1f]/g, '');
        this.#res = options.request.raw.res;
        this.#buffer = new EventBuffer();
        this.#retry = options.retry;
        this.#keepAlive = options.keepAlive;
        this.#headers = options.headers;
        this.#backpressure = options.backpressure;
        this.#maxDuration = options.maxDuration;
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
        this.#res.writeHead(200, {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            Connection: 'keep-alive',
            'X-Accel-Buffering': 'no',
            ...this.#headers,
        });

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
        if (this.#closed) {
            return;
        }

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
            const pendingBytes = this.#res.writableLength + Buffer.byteLength(payload, 'utf8');

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

        this.#res.end();
    }

    /** @internal */
    #flush(): boolean {
        const data = this.#buffer.read();

        if (data) {
            try {
                this.#res.write(data);
            } catch {
                this.#buffer.clear();
                this.close();

                return false;
            }

            this.#buffer.clear();
        }

        return true;
    }
}
