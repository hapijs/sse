import { expect, describe, it, afterEach } from 'vitest';

import { FiniteReplayer, ValidReplayer } from './replayer.ts';

describe('FiniteReplayer', () => {
    it('records and replays entries after lastEventId', () => {
        const replayer = new FiniteReplayer({ size: 10 });

        replayer.record({ data: 'a', id: '1' });
        replayer.record({ data: 'b', id: '2' });
        replayer.record({ data: 'c', id: '3' });

        const entries = replayer.replay('1');

        expect(entries.length).toBe(2);
        expect(entries[0]).toEqual({ data: 'b', event: undefined, id: '2' });
        expect(entries[1]).toEqual({ data: 'c', event: undefined, id: '3' });
    });

    it('returns all entries when lastEventId is not found', () => {
        const replayer = new FiniteReplayer({ size: 10 });

        replayer.record({ data: 'a', id: '1' });
        replayer.record({ data: 'b', id: '2' });

        const entries = replayer.replay('999');

        expect(entries.length).toBe(2);
    });

    it('returns empty array when lastEventId is the latest', () => {
        const replayer = new FiniteReplayer({ size: 10 });

        replayer.record({ data: 'a', id: '1' });

        const entries = replayer.replay('1');

        expect(entries.length).toBe(0);
    });

    it('evicts oldest entries when size exceeded', () => {
        const replayer = new FiniteReplayer({ size: 3 });

        replayer.record({ data: 'a', id: '1' });
        replayer.record({ data: 'b', id: '2' });
        replayer.record({ data: 'c', id: '3' });
        replayer.record({ data: 'd', id: '4' });

        const entries = replayer.replay('0');

        expect(entries.length).toBe(3);
        expect(entries[0].id).toBe('2');
        expect(entries[2].id).toBe('4');
    });

    it('preserves event field', () => {
        const replayer = new FiniteReplayer({ size: 10 });

        replayer.record({ data: 'a', event: 'chat', id: '1' });

        const entries = replayer.replay('0');

        expect(entries[0].event).toBe('chat');
    });

    it('autoId generates sequential ids when entry id is empty', () => {
        const replayer = new FiniteReplayer({ size: 10, autoId: true });

        replayer.record({ data: 'a', id: '' });
        replayer.record({ data: 'b', id: '' });

        const entries = replayer.replay('0');

        expect(entries[0].id).toBe('1');
        expect(entries[1].id).toBe('2');
    });

    it('autoId does not overwrite explicit ids', () => {
        const replayer = new FiniteReplayer({ size: 10, autoId: true });

        replayer.record({ data: 'a', id: 'custom-1' });

        const entries = replayer.replay('0');

        expect(entries[0].id).toBe('custom-1');
    });

    it('returns copy of buffer to prevent mutation', () => {
        const replayer = new FiniteReplayer({ size: 10 });

        replayer.record({ data: 'a', id: '1' });

        const entries1 = replayer.replay('0');
        const entries2 = replayer.replay('0');

        expect(entries1).not.toBe(entries2);
        expect(entries1).toEqual(entries2);
    });
});

describe('ValidReplayer', () => {
    let replayer: ValidReplayer;

    afterEach(() => {
        if (replayer) {
            replayer.stop();
        }
    });

    it('records and replays entries after lastEventId', () => {
        replayer = new ValidReplayer({ ttl: 60_000 });

        replayer.record({ data: 'a', id: '1' });
        replayer.record({ data: 'b', id: '2' });
        replayer.record({ data: 'c', id: '3' });

        const entries = replayer.replay('1');

        expect(entries.length).toBe(2);
        expect(entries[0]).toEqual({ data: 'b', event: undefined, id: '2' });
        expect(entries[1]).toEqual({ data: 'c', event: undefined, id: '3' });
    });

    it('returns all entries when lastEventId is not found', () => {
        replayer = new ValidReplayer({ ttl: 60_000 });

        replayer.record({ data: 'a', id: '1' });
        replayer.record({ data: 'b', id: '2' });

        const entries = replayer.replay('999');

        expect(entries.length).toBe(2);
    });

    it('expires entries after ttl', async () => {
        replayer = new ValidReplayer({ ttl: 50 });

        replayer.record({ data: 'a', id: '1' });

        await new Promise((r) => setTimeout(r, 100));

        const entries = replayer.replay('0');

        expect(entries.length).toBe(0);
    });

    it('preserves non-expired entries', async () => {
        replayer = new ValidReplayer({ ttl: 5000 });

        replayer.record({ data: 'a', id: '1' });

        const entries = replayer.replay('0');

        expect(entries.length).toBe(1);
    });

    it('autoId generates sequential ids when entry id is empty', () => {
        replayer = new ValidReplayer({ ttl: 60_000, autoId: true });

        replayer.record({ data: 'a', id: '' });
        replayer.record({ data: 'b', id: '' });

        const entries = replayer.replay('0');

        expect(entries[0].id).toBe('1');
        expect(entries[1].id).toBe('2');
    });

    it('stop() clears the timer', () => {
        replayer = new ValidReplayer({ ttl: 1000 });

        replayer.stop();
        replayer.stop();
    });

    it('replay strips internal expiresAt field', () => {
        replayer = new ValidReplayer({ ttl: 60_000 });

        replayer.record({ data: 'a', id: '1' });

        const entries = replayer.replay('0');

        expect(Object.keys(entries[0])).toEqual(['data', 'event', 'id']);
    });
});
