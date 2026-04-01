import type { RouteOptions } from '@hapi/hapi';

import { Session } from './session.js';
import type { Replayer } from './replayer.js';

export interface FilterOptions {
    credentials: unknown;
    params: Record<string, string>;
    internal: unknown;
}

export interface SubscriptionConfig<T = unknown> {
    auth?: RouteOptions['auth'];
    filter?: (
        path: string,
        message: T,
        options: FilterOptions,
    ) => boolean | { override: unknown } | Promise<boolean | { override: unknown }>;
    onSubscribe?: (session: Session, path: string, params: Record<string, string>) => void | Promise<void>;
    onUnsubscribe?: (session: Session, path: string, params: Record<string, string>) => void;
    onReconnect?: (session: Session, path: string, params: Record<string, string>) => void | Promise<void>;
    retry?: number | null;
    keepAlive?: { interval: number } | false;
    replay?: Replayer;
    maxSessions?: number;
    maxDuration?: number;
}

export interface SubscriptionInfo {
    pattern: string;
    activeSessions: number;
}

interface CompiledSubscription {
    pattern: string;
    regex: RegExp;
    paramNames: string[];
    config: SubscriptionConfig;
    sessions: Set<Session>;
}

interface SessionInfo {
    pattern: string;
    params: Record<string, string>;
    resolvedPath: string;
}

export class SubscriptionRegistry {
    /** @internal */
    readonly #subscriptions = new Map<string, CompiledSubscription>();
    /** @internal */
    readonly #sessionInfo = new Map<Session, SessionInfo>();

    register(pattern: string, config: SubscriptionConfig): void {
        const paramNames: string[] = [];

        const regexStr = pattern.replace(/\{([^}]+)\}/g, (_, name) => {
            paramNames.push(name);

            return '([^/]+)';
        });

        this.#subscriptions.set(pattern, {
            pattern,
            regex: new RegExp(`^${regexStr}$`),
            paramNames,
            config,
            sessions: new Set(),
        });
    }

    getConfig(pattern: string): SubscriptionConfig | undefined {
        return this.#subscriptions.get(pattern)?.config;
    }

    matchPath(path: string): { pattern: string; params: Record<string, string>; config: SubscriptionConfig } | null {
        for (const sub of this.#subscriptions.values()) {
            const match = sub.regex.exec(path);

            if (match) {
                const params: Record<string, string> = {};

                for (let i = 0; i < sub.paramNames.length; i++) {
                    params[sub.paramNames[i]] = match[i + 1];
                }

                return { pattern: sub.pattern, params, config: sub.config };
            }
        }

        return null;
    }

    addSession(pattern: string, session: Session, params: Record<string, string>, resolvedPath: string): boolean {
        const sub = this.#subscriptions.get(pattern);

        if (!sub) {
            return false;
        }

        if (sub.config.maxSessions && sub.sessions.size >= sub.config.maxSessions) {
            return false;
        }

        sub.sessions.add(session);
        this.#sessionInfo.set(session, { pattern, params, resolvedPath });

        return true;
    }

    removeSession(session: Session): void {
        const info = this.#sessionInfo.get(session);

        if (info) {
            this.#subscriptions.get(info.pattern)?.sessions.delete(session);
            this.#sessionInfo.delete(session);
        }
    }

    getSessionInfo(session: Session): SessionInfo | undefined {
        return this.#sessionInfo.get(session);
    }

    get sessionCount(): number {
        return this.#sessionInfo.size;
    }

    subscriptionSessionCount(pattern: string): number {
        return this.#subscriptions.get(pattern)?.sessions.size ?? 0;
    }

    listSubscriptions(): SubscriptionInfo[] {
        const result: SubscriptionInfo[] = [];

        for (const sub of this.#subscriptions.values()) {
            result.push({ pattern: sub.pattern, activeSessions: sub.sessions.size });
        }

        return result;
    }

    async publish<T = unknown>(
        path: string,
        data: T,
        opts?: { event?: string; id?: string; internal?: unknown; matchMode?: 'pattern' | 'literal' },
    ): Promise<number> {
        const matched = this.matchPath(path);

        if (!matched) {
            return 0;
        }

        const sub = this.#subscriptions.get(matched.pattern)!;
        const sessions = [...sub.sessions];
        const isLiteral = opts?.matchMode === 'literal';
        let delivered = 0;

        for (const session of sessions) {
            const info = this.#sessionInfo.get(session);

            if (!info) {
                continue;
            }

            if (isLiteral && info.resolvedPath !== path) {
                continue;
            }

            try {
                if (sub.config.filter) {
                    const result = await sub.config.filter(path, data as never, {
                        credentials: session.request.auth.credentials,
                        params: info.params,
                        internal: opts?.internal,
                    });

                    if (result === false) {
                        continue;
                    }

                    if (typeof result === 'object' && result !== null && 'override' in result) {
                        if (session.push(result.override, opts?.event, opts?.id)) {
                            delivered++;
                        }

                        continue;
                    }
                }

                if (session.push(data, opts?.event, opts?.id)) {
                    delivered++;
                }
            } catch {
                // Filter error for one session must not block delivery to others
            }
        }

        if (sub.config.replay && opts?.id) {
            sub.config.replay.record({ data, event: opts.event, id: opts.id });
        }

        return delivered;
    }

    async broadcast(data: unknown, opts?: { event?: string; id?: string }): Promise<number> {
        let delivered = 0;

        for (const sub of this.#subscriptions.values()) {
            for (const session of [...sub.sessions]) {
                if (session.push(data, opts?.event, opts?.id)) {
                    delivered++;
                }
            }
        }

        return delivered;
    }

    async eachSession(fn: (session: Session) => void | Promise<void>, opts?: { subscription?: string }): Promise<void> {
        if (opts?.subscription) {
            const sub = this.#subscriptions.get(opts.subscription);

            if (sub) {
                for (const session of [...sub.sessions]) {
                    await fn(session);
                }
            }

            return;
        }

        for (const sub of this.#subscriptions.values()) {
            for (const session of [...sub.sessions]) {
                await fn(session);
            }
        }
    }

    closeSessions(pattern: string): void {
        const sub = this.#subscriptions.get(pattern);

        if (!sub) {
            return;
        }

        for (const session of sub.sessions) {
            this.#sessionInfo.delete(session);
            session.close();
        }

        sub.sessions.clear();
    }

    closeAll(): void {
        for (const sub of this.#subscriptions.values()) {
            if (sub.config.replay?.stop) {
                sub.config.replay.stop();
            }

            for (const session of sub.sessions) {
                session.close();
            }

            sub.sessions.clear();
        }

        this.#sessionInfo.clear();
    }
}
