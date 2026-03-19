export class EventBuffer {
    /** @internal */
    #buffer = '';

    data(value: unknown): this {
        const str = typeof value === 'string' ? value : (JSON.stringify(value) ?? '');
        const normalized = str.replace(/\r\n|\r/g, '\n');

        for (const line of normalized.split('\n')) {
            this.#buffer += `data: ${line}\n`;
        }

        return this;
    }

    event(type: string): this {
        this.#buffer += `event: ${type.replace(/[\r\n]/g, '')}\n`;

        return this;
    }

    id(id: string): this {
        if (id.includes('\0')) {
            throw new Error('Event ID must not contain null characters');
        }

        this.#buffer += `id: ${id.replace(/[\r\n]/g, '')}\n`;

        return this;
    }

    retry(ms: number): this {
        if (!Number.isInteger(ms) || ms < 0) {
            throw new Error('Retry must be a non-negative integer');
        }

        this.#buffer += `retry: ${ms}\n`;

        return this;
    }

    comment(text?: string): this {
        if (text) {
            const normalized = text.replace(/\r\n|\r/g, '\n');

            for (const line of normalized.split('\n')) {
                this.#buffer += `: ${line}\n`;
            }
        } else {
            this.#buffer += ':\n';
        }

        return this;
    }

    dispatch(): this {
        this.#buffer += '\n';

        return this;
    }

    push(data: unknown, event?: string, id?: string): this {
        if (event) {
            this.event(event);
        }

        if (id != null) {
            this.id(id);
        }

        this.data(data);
        this.dispatch();

        return this;
    }

    read(): string {
        return this.#buffer;
    }

    clear(): void {
        this.#buffer = '';
    }
}
