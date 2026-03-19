import { expect, describe, it } from 'vitest';
import { SubscriptionRegistry } from '../src/subscription.js';
import { Session } from '../src/session.js';

describe.concurrent('SubscriptionRegistry', () => {
    it('getSessionInfo returns session info', () => {
        const registry = new SubscriptionRegistry();
        const session = { request: { headers: {} } } as any;
        registry.register('/events', {});
        registry.addSession('/events', session, { id: '1' }, '/events/1');

        const info = registry.getSessionInfo(session);
        expect(info).toEqual({
            pattern: '/events',
            params: { id: '1' },
            resolvedPath: '/events/1'
        });
    });

    it('getConfig returns config', () => {
        const registry = new SubscriptionRegistry();
        const config = {};
        registry.register('/events', config);

        expect(registry.getConfig('/events')).toBe(config);
    });

    it('removeSession is idempotent for non-existent session', () => {
        const registry = new SubscriptionRegistry();
        const session = new Session({
            request: {
                headers: {},
                raw: { req: { socket: {} }, res: { writeHead: () => {}, end: () => {} } }
            } as any,
            retry: null,
            keepAlive: false,
            headers: {},
        });

        // Should not throw
        registry.removeSession(session);
    });

    it('matchPath returns null for no match', () => {
        const registry = new SubscriptionRegistry();
        expect(registry.matchPath('/nonexistent')).toBeNull();
    });

    it('publish continues if session info is missing', async () => {
        const registry = new SubscriptionRegistry();
        const session = { request: { headers: {} }, push: () => true } as any;
        registry.register('/a', {});
        registry.register('/b', {});

        // Add session to both patterns
        registry.addSession('/a', session, {}, '/a');
        // This sets #sessionInfo.get(session).pattern = '/a'

        registry.addSession('/b', session, {}, '/b');
        // This overwrites #sessionInfo.get(session).pattern = '/b'
        // But session is still in /a's sub.sessions!

        // Now remove session
        registry.removeSession(session);
        // This looks up info (pattern='/b'), deletes from /b's sub.sessions, and deletes from #sessionInfo.
        // session is STILL in /a's sub.sessions, but NOT in #sessionInfo!

        const delivered = await registry.publish('/a', 'data');
        expect(delivered).toBe(0);
    });

    it('addSession does nothing for a non-existent subscription', () => {
        const registry = new SubscriptionRegistry();
        const session = { request: { headers: {} } } as any;

        // No subscription registered for '/events'
        registry.addSession('/events', session, {}, '/events');
        expect(registry.getSessionInfo(session)).toBeUndefined();
    });

    it('publish correctly handles failed pushes with filter override', async () => {
        const registry = new SubscriptionRegistry();
        const session = {
            request: { auth: { credentials: {} } },
            push: () => false // Simulate closed session or error during push
        } as any;

        registry.register('/events', {
            filter: () => ({ override: 'new-data' })
        });
        registry.addSession('/events', session, {}, '/events');

        const delivered = await registry.publish('/events', 'data');
        expect(delivered).toBe(0);
    });

    it('broadcast handles failed pushes to a session', async () => {
        const registry = new SubscriptionRegistry();
        const session = {
            push: () => false // Simulate closed session or error
        } as any;

        registry.register('/events', {});
        registry.addSession('/events', session, {}, '/events');

        const delivered = await registry.broadcast('data');
        expect(delivered).toBe(0);
    });
});
