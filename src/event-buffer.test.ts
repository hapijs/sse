import { expect, describe, it } from 'vitest';

import { EventBuffer } from './event-buffer.ts';

describe('EventBuffer', () => {
    it('serializes string data', () => {
        const buf = new EventBuffer();

        buf.data('hello');

        expect(buf.read()).toBe('data: hello\n');
    });

    it('splits multi-line data into multiple data fields', () => {
        const buf = new EventBuffer();

        buf.data('hello\nworld');

        expect(buf.read()).toBe('data: hello\ndata: world\n');
    });

    it('JSON-stringifies non-string data', () => {
        const buf = new EventBuffer();

        buf.data({ foo: 1 });

        expect(buf.read()).toBe('data: {"foo":1}\n');
    });

    it('writes event field', () => {
        const buf = new EventBuffer();

        buf.event('update');

        expect(buf.read()).toBe('event: update\n');
    });

    it('writes id field', () => {
        const buf = new EventBuffer();

        buf.id('42');

        expect(buf.read()).toBe('id: 42\n');
    });

    it('throws on id containing null character', () => {
        const buf = new EventBuffer();

        expect(() => buf.id('abc\0def')).toThrow('null');
    });

    it('writes retry field', () => {
        const buf = new EventBuffer();

        buf.retry(3000);

        expect(buf.read()).toBe('retry: 3000\n');
    });

    it('throws on negative retry', () => {
        const buf = new EventBuffer();

        expect(() => buf.retry(-1)).toThrow('non-negative integer');
    });

    it('throws on NaN retry', () => {
        const buf = new EventBuffer();

        expect(() => buf.retry(NaN)).toThrow('non-negative integer');
    });

    it('throws on float retry', () => {
        const buf = new EventBuffer();

        expect(() => buf.retry(1.5)).toThrow('non-negative integer');
    });

    it('writes comment with text', () => {
        const buf = new EventBuffer();

        buf.comment('keepalive');

        expect(buf.read()).toBe(': keepalive\n');
    });

    it('writes empty comment', () => {
        const buf = new EventBuffer();

        buf.comment();

        expect(buf.read()).toBe(':\n');
    });

    it('appends blank line on dispatch', () => {
        const buf = new EventBuffer();

        buf.data('hi');
        buf.dispatch();

        expect(buf.read()).toBe('data: hi\n\n');
    });

    it('push() produces complete event block', () => {
        const buf = new EventBuffer();

        buf.push({ msg: 'hello' }, 'chat', '1');

        expect(buf.read()).toBe('event: chat\nid: 1\ndata: {"msg":"hello"}\n\n');
    });

    it('push() works without event and id', () => {
        const buf = new EventBuffer();

        buf.push('simple');

        expect(buf.read()).toBe('data: simple\n\n');
    });

    it('clear() resets the buffer', () => {
        const buf = new EventBuffer();

        buf.data('hello');
        buf.clear();

        expect(buf.read()).toBe('');
    });

    it('normalizes CRLF in data to multiple data fields', () => {
        const buf = new EventBuffer();

        buf.data('hello\r\nworld');

        expect(buf.read()).toBe('data: hello\ndata: world\n');
    });

    it('normalizes CR in data to multiple data fields', () => {
        const buf = new EventBuffer();

        buf.data('hello\rworld');

        expect(buf.read()).toBe('data: hello\ndata: world\n');
    });

    it('handles empty string data', () => {
        const buf = new EventBuffer();

        buf.data('');

        expect(buf.read()).toBe('data: \n');
    });

    it('throws on Infinity retry', () => {
        const buf = new EventBuffer();

        expect(() => buf.retry(Infinity)).toThrow('non-negative integer');
    });

    it('handles JSON with embedded newlines (escaped by stringify)', () => {
        const buf = new EventBuffer();

        buf.data({ text: 'a\nb' });

        expect(buf.read()).toBe('data: {"text":"a\\nb"}\n');
    });

    it('accumulates multiple pushes', () => {
        const buf = new EventBuffer();

        buf.push('first', 'a');
        buf.push('second', 'b');

        expect(buf.read()).toBe('event: a\ndata: first\n\nevent: b\ndata: second\n\n');
    });

    it('push() with empty string id sends id field (resets client lastEventId per spec)', () => {
        const buf = new EventBuffer();

        buf.push('data', 'evt', '');

        expect(buf.read()).toBe('event: evt\nid: \ndata: data\n\n');
    });

    it('push() with undefined id skips id field', () => {
        const buf = new EventBuffer();

        buf.push('data', 'evt');

        expect(buf.read()).toBe('event: evt\ndata: data\n\n');
    });

    it('event() strips newlines to prevent field injection', () => {
        const buf = new EventBuffer();

        buf.event('foo\ndata: injected');

        expect(buf.read()).toBe('event: foodata: injected\n');
    });

    it('id() strips newlines to prevent field injection', () => {
        const buf = new EventBuffer();

        buf.id('abc\ndef');

        expect(buf.read()).toBe('id: abcdef\n');
    });

    it('id() strips CR from value', () => {
        const buf = new EventBuffer();

        buf.id('abc\rdef');

        expect(buf.read()).toBe('id: abcdef\n');
    });

    it('comment() splits multi-line text into multiple comment lines', () => {
        const buf = new EventBuffer();

        buf.comment('line1\nline2');

        expect(buf.read()).toBe(': line1\n: line2\n');
    });

    it('comment() handles CRLF in text', () => {
        const buf = new EventBuffer();

        buf.comment('a\r\nb');

        expect(buf.read()).toBe(': a\n: b\n');
    });

    it('data(null) serializes as "null"', () => {
        const buf = new EventBuffer();

        buf.data(null);

        expect(buf.read()).toBe('data: null\n');
    });

    it('data(undefined) serializes as empty string', () => {
        const buf = new EventBuffer();

        buf.data(undefined);

        expect(buf.read()).toBe('data: \n');
    });

    it('data with number serializes correctly', () => {
        const buf = new EventBuffer();

        buf.data(42);

        expect(buf.read()).toBe('data: 42\n');
    });

    it('data with boolean serializes correctly', () => {
        const buf = new EventBuffer();

        buf.data(true);

        expect(buf.read()).toBe('data: true\n');
    });

    it('data with trailing newline produces extra data line', () => {
        const buf = new EventBuffer();

        buf.data('hello\n');

        expect(buf.read()).toBe('data: hello\ndata: \n');
    });

    it('retry with zero is valid', () => {
        const buf = new EventBuffer();

        buf.retry(0);

        expect(buf.read()).toBe('retry: 0\n');
    });

    it('event() strips CR without LF', () => {
        const buf = new EventBuffer();

        buf.event('test\rinjection');

        expect(buf.read()).toBe('event: testinjection\n');
    });

    it('event() strips CRLF', () => {
        const buf = new EventBuffer();

        buf.event('test\r\ninjection');

        expect(buf.read()).toBe('event: testinjection\n');
    });
});
