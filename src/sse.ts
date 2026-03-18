import type { NamedPlugin, Request, ResponseToolkit, RouteOptions, Lifecycle } from '@hapi/hapi';
import { createRequire } from 'node:module';

import { Session } from './session.ts';
import type { BackpressureOptions } from './session.ts';
import { SubscriptionRegistry } from './subscription.ts';

const { version } = createRequire(import.meta.url)('../package.json') as { version: string };
import type { SubscriptionConfig, SubscriptionInfo, FilterOptions } from './subscription.ts';

export type { SubscriptionConfig, SubscriptionInfo, FilterOptions };

export interface SseHooks {
    onSession?: (session: Session, path: string, params: Record<string, string>) => void;
    onSessionClose?: (session: Session, path: string, params: Record<string, string>) => void;
    onPublish?: (path: string, data: unknown, deliveryCount: number) => void;
}

export interface SsePluginOptions {
    keepAlive?: { interval: number } | false;
    retry?: number | null;
    headers?: Record<string, string>;
    hooks?: SseHooks;
    backpressure?: BackpressureOptions;
}

export interface SseHandlerOptions {
    stream: (request: Request, session: Session) => void | Promise<void>;
    retry?: number | null;
    keepAlive?: { interval: number } | false;
    headers?: Record<string, string>;
    backpressure?: BackpressureOptions;
}

export interface SseStats {
    totalConnections: number;
    totalDisconnections: number;
    totalPublishes: number;
    totalBroadcasts: number;
    totalEventsDelivered: number;
    activeSessions: number;
}

export interface SseApi {
    subscription: <T = unknown>(path: string, config?: SubscriptionConfig<T>) => void;
    publish: <T = unknown>(
        path: string,
        data: T,
        opts?: { event?: string; id?: string; internal?: unknown; matchMode?: 'pattern' | 'literal' },
    ) => Promise<number>;
    broadcast: (data: unknown, opts?: { event?: string; id?: string }) => Promise<number>;
    eachSession: (fn: (session: Session) => void | Promise<void>, opts?: { subscription?: string }) => Promise<void>;
    subscriptions: () => SubscriptionInfo[];
    closeSessions: (pattern: string) => void;
    readonly sessionCount: number;
    stats: () => SseStats;
}

declare module '@hapi/hapi' {
    interface Server {
        sse: SseApi;
    }

    interface HandlerDecorations {
        sse?: SseHandlerOptions;
    }
}

const defaults: Required<Omit<SsePluginOptions, 'hooks' | 'backpressure'>> = {
    keepAlive: { interval: 15_000 },
    retry: 2000,
    headers: {},
};

export const SsePlugin: NamedPlugin<SsePluginOptions> = {
    name: '@hapi/sse',
    version,
    register: (server, options) => {
        const config = { ...defaults, ...options };
        const registry = new SubscriptionRegistry();
        const hooks = options.hooks;

        let totalConnections = 0;
        let totalDisconnections = 0;
        let totalPublishes = 0;
        let totalBroadcasts = 0;
        let totalEventsDelivered = 0;

        const api: SseApi = {
            subscription: (path, subConfig = {}) => {
                registry.register(path, subConfig as SubscriptionConfig);

                const routeConfig: RouteOptions = {};

                if (subConfig.auth !== undefined) {
                    routeConfig.auth = subConfig.auth;
                }

                server.route({
                    method: 'GET',
                    path,
                    options: routeConfig,
                    handler: async (request: Request, h: ResponseToolkit) => {
                        const matched = registry.matchPath(request.path)!;

                        const session = new Session({
                            request,
                            retry: subConfig.retry ?? config.retry,
                            keepAlive: subConfig.keepAlive ?? config.keepAlive,
                            headers: config.headers,
                            backpressure: options.backpressure,
                        });

                        if (subConfig.onSubscribe) {
                            await subConfig.onSubscribe(session, request.path, matched.params);
                        }

                        session.initialize();
                        registry.addSession(matched.pattern, session, matched.params, request.path);
                        totalConnections++;

                        if (hooks?.onSession) {
                            try {
                                hooks.onSession(session, request.path, matched.params);
                            } catch {
                                /* hooks must not break SSE */
                            }
                        }

                        const replayer = (subConfig as SubscriptionConfig).replay;

                        if (session.lastEventId && replayer) {
                            const entries = replayer.replay(session.lastEventId);

                            for (const entry of entries) {
                                session.push(entry.data, entry.event, entry.id);
                            }
                        }

                        if (session.lastEventId && subConfig.onReconnect) {
                            try {
                                await subConfig.onReconnect(session, request.path, matched.params);
                            } catch {
                                registry.removeSession(session);
                                session.close();
                            }
                        }

                        request.raw.req.once('close', () => {
                            totalDisconnections++;

                            if (hooks?.onSessionClose) {
                                try {
                                    hooks.onSessionClose(session, request.path, matched.params);
                                } catch {
                                    /* hooks must not break SSE */
                                }
                            }

                            try {
                                if (subConfig.onUnsubscribe) {
                                    subConfig.onUnsubscribe(session, request.path, matched.params);
                                }
                            } catch {
                                // onUnsubscribe errors must not break cleanup
                            }

                            registry.removeSession(session);
                            session.close();
                        });

                        return h.abandon;
                    },
                });
            },
            publish: async (path, data, opts) => {
                const count = await registry.publish(path, data, opts);

                totalPublishes++;
                totalEventsDelivered += count;

                if (hooks?.onPublish) {
                    try {
                        hooks.onPublish(path, data, count);
                    } catch {
                        /* hooks must not break SSE */
                    }
                }

                return count;
            },
            broadcast: async (data, opts) => {
                const count = await registry.broadcast(data, opts);

                totalBroadcasts++;
                totalEventsDelivered += count;

                return count;
            },
            eachSession: (fn, opts) => registry.eachSession(fn, opts),
            subscriptions: () => registry.listSubscriptions(),
            closeSessions: (pattern) => registry.closeSessions(pattern),
            get sessionCount() {
                return registry.sessionCount;
            },
            stats: () => ({
                totalConnections,
                totalDisconnections,
                totalPublishes,
                totalBroadcasts,
                totalEventsDelivered,
                activeSessions: registry.sessionCount,
            }),
        };

        server.decorate('server', 'sse', api);

        server.decorate('handler', 'sse', (_route: unknown, handlerOptions: SseHandlerOptions) => {
            return async (request: Request, h: ResponseToolkit): Promise<Lifecycle.ReturnValue> => {
                const session = new Session({
                    request,
                    retry: handlerOptions.retry ?? config.retry,
                    keepAlive: handlerOptions.keepAlive ?? config.keepAlive,
                    headers: handlerOptions.headers ?? config.headers,
                    backpressure: handlerOptions.backpressure ?? options.backpressure,
                });

                session.initialize();

                request.raw.req.once('close', () => {
                    session.close();
                });

                try {
                    await handlerOptions.stream(request, session);
                } catch {
                    session.close();
                }

                return h.abandon;
            };
        });

        server.ext('onPreStop', () => {
            registry.closeAll();
        });
    },
};
