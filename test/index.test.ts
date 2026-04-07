import { describe, expect, it } from 'vitest';
import * as SSE from '../src/index.js';

describe.concurrent('SSE', () => {
    it('index exports', () => {
        expect(SSE.EventBuffer).toBeDefined();
        expect(SSE.Session).toBeDefined();
        expect(SSE.SsePlugin).toBeDefined();
        expect(SSE.FiniteReplayer).toBeDefined();
        expect(SSE.ValidReplayer).toBeDefined();
    });
});
