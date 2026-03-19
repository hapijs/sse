export interface ReplayEntry {
    data: unknown;
    event?: string;
    id: string;
}

export interface Replayer {
    record(entry: ReplayEntry): void;
    replay(lastEventId: string): ReplayEntry[];
    stop?(): void;
}

export class FiniteReplayer implements Replayer {
    /** @internal */
    readonly #size: number;
    /** @internal */
    readonly #autoId: boolean;
    /** @internal */
    readonly #buffer: ReplayEntry[] = [];
    /** @internal */
    #counter = 0;

    constructor(opts: { size: number; autoId?: boolean }) {
        this.#size = opts.size;
        this.#autoId = opts.autoId ?? false;
    }

    record(entry: ReplayEntry): void {
        const stored: ReplayEntry = {
            data: entry.data,
            event: entry.event,
            id: entry.id || (this.#autoId ? String(++this.#counter) : entry.id),
        };

        if (this.#buffer.length >= this.#size) {
            this.#buffer.shift();
        }

        this.#buffer.push(stored);
    }

    replay(lastEventId: string): ReplayEntry[] {
        const idx = this.#buffer.findIndex((e) => e.id === lastEventId);

        if (idx === -1) {
            return [...this.#buffer];
        }

        return this.#buffer.slice(idx + 1);
    }
}

interface TimedEntry extends ReplayEntry {
    expiresAt: number;
}

export class ValidReplayer implements Replayer {
    /** @internal */
    readonly #ttl: number;
    /** @internal */
    readonly #autoId: boolean;
    /** @internal */
    readonly #buffer: TimedEntry[] = [];
    /** @internal */
    #timer: ReturnType<typeof setInterval> | null = null;
    /** @internal */
    #counter = 0;

    constructor(opts: { ttl: number; autoId?: boolean }) {
        this.#ttl = opts.ttl;
        this.#autoId = opts.autoId ?? false;

        this.#timer = setInterval(() => this.#gc(), Math.max(this.#ttl / 2, 100));
    }

    record(entry: ReplayEntry): void {
        const stored: TimedEntry = {
            data: entry.data,
            event: entry.event,
            id: entry.id || (this.#autoId ? String(++this.#counter) : entry.id),
            expiresAt: Date.now() + this.#ttl,
        };

        this.#buffer.push(stored);
    }

    replay(lastEventId: string): ReplayEntry[] {
        this.#gc();

        const idx = this.#buffer.findIndex((e) => e.id === lastEventId);

        if (idx === -1) {
            return this.#buffer.map(({ data, event, id }) => ({ data, event, id }));
        }

        return this.#buffer.slice(idx + 1).map(({ data, event, id }) => ({ data, event, id }));
    }

    stop(): void {
        if (this.#timer) {
            clearInterval(this.#timer);
            this.#timer = null;
        }
    }

    /** @internal */
    #gc(): void {
        const now = Date.now();

        while (this.#buffer.length > 0 && this.#buffer[0].expiresAt <= now) {
            this.#buffer.shift();
        }
    }
}
