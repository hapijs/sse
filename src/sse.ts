import type { NamedPlugin, Request, RequestRoute, ResponseToolkit, RouteOptions, Lifecycle } from '@hapi/hapi';
import Boom from '@hapi/boom';
import * as Hoek from '@hapi/hoek';
import Joi from 'joi';
import { createRequire } from 'node:module';

import { Session } from './session.js';
import type { BackpressureOptions } from './session.js';
import { SubscriptionRegistry } from './subscription.js';

const { version } = createRequire(import.meta.url)('../package.json') as { version: string };
import type { SubscriptionConfig, SubscriptionInfo, FilterOptions } from './subscription.js';

export type { SubscriptionConfig, SubscriptionInfo, FilterOptions };

export interface SseHooks {
    onSession?: (session: Session, path: string, params: Record<string, string>) => void;
    onSessionClose?: (session: Session, path: string, params: Record<string, string>) => void;
    onPublish?: (path: string, data: unknown, deliveryCount: number) => void;
}

export interface CompletionCacheOptions {
    cache?: string;
    segment?: string;
    expiresIn?: number;
}

export interface CompletionStore {
    set(id: string, value: boolean, ttl: number): Promise<void>;
}

export interface SsePluginOptions {
    keepAlive?: { interval: number } | false;
    retry?: number | null;
    headers?: Record<string, string>;
    hooks?: SseHooks;
    backpressure?: BackpressureOptions;
    completion?: CompletionCacheOptions;
}

export interface SseHandlerOptions {
    stream: (request: Request, session: Session) => void | Promise<void>;
    retry?: number | null;
    keepAlive?: { interval: number } | false;
    headers?: Record<string, string>;
    backpressure?: BackpressureOptions;
    maxDuration?: number;
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

    interface PluginsStates {
        '@hapi/sse'?: {
            completionStore: CompletionStore;
        };
    }
}

const RETRY_FLOOR = 1000;

const clampRetry = (value: number | null): number | null => {
    return value === null ? null : Math.max(value, RETRY_FLOOR);
};

const COMPLETION_DEFAULTS: CompletionCacheOptions = {
    segment: 'completed-sse-sessions',
    expiresIn: 5 * 60 * 1000,
};

const defaults: Required<Omit<SsePluginOptions, 'hooks' | 'backpressure' | 'completion'>> = {
    keepAlive: { interval: 15_000 },
    retry: 2000,
    headers: {},
};

const keepAliveSchema = Joi.alternatives().try(
    Joi.object({ interval: Joi.number().integer().positive().required() }),
    Joi.valid(false),
);

const retrySchema = Joi.number().integer().min(0).allow(null);

const headersSchema = Joi.object().pattern(Joi.string(), Joi.string());

const backpressureSchema = Joi.object({
    maxBytes: Joi.number().integer().positive().required(),
    strategy: Joi.string().valid('close', 'drop').required(),
});

const hooksSchema = Joi.object({
    onSession: Joi.function(),
    onSessionClose: Joi.function(),
    onPublish: Joi.function(),
});

const completionSchema = Joi.object({
    cache: Joi.string(),
    segment: Joi.string(),
    expiresIn: Joi.number().integer().positive(),
});

const pluginOptionsSchema = Joi.object({
    keepAlive: keepAliveSchema,
    retry: retrySchema,
    headers: headersSchema,
    hooks: hooksSchema,
    backpressure: backpressureSchema,
    completion: completionSchema,
}).label('SsePluginOptions');

const replayerSchema = Joi.object({
    record: Joi.function().required(),
    replay: Joi.function().required(),
    stop: Joi.function(),
})
    .unknown(true)
    .label('Replayer');

const subscriptionConfigSchema = Joi.object({
    auth: Joi.any(),
    filter: Joi.function(),
    refuse: Joi.function(),
    onSubscribe: Joi.function(),
    onUnsubscribe: Joi.function(),
    onReconnect: Joi.function(),
    retry: retrySchema,
    keepAlive: keepAliveSchema,
    replay: replayerSchema,
    maxSessions: Joi.number().integer().positive(),
    maxDuration: Joi.number().integer().positive(),
}).label('SubscriptionConfig');

const handlerOptionsSchema = Joi.object({
    stream: Joi.function().required(),
    retry: retrySchema,
    keepAlive: keepAliveSchema,
    headers: headersSchema,
    backpressure: backpressureSchema,
    maxDuration: Joi.number().integer().positive(),
}).label('SseHandlerOptions');

export const SsePlugin: NamedPlugin<SsePluginOptions> = {
    name: '@hapi/sse',
    version,
    register: (server, options) => {
        Joi.attempt(options, pluginOptionsSchema, 'Invalid @hapi/sse plugin options:');

        const config = { ...defaults, ...options };
        const registry = new SubscriptionRegistry();
        const hooks = options.hooks;

        const completion = { ...COMPLETION_DEFAULTS, ...options.completion };
        const completionStore = server.cache<boolean>(completion);

        server.realm.plugins['@hapi/sse'] = { completionStore };

        let totalConnections = 0;
        let totalDisconnections = 0;
        let totalPublishes = 0;
        let totalBroadcasts = 0;
        let totalEventsDelivered = 0;

        const api: SseApi = {
            subscription: (path, subConfig = {}) => {
                Hoek.assert(
                    typeof path === 'string' && path.length > 0,
                    'sse.subscription(path): path must be a non-empty string',
                );
                Hoek.assert(
                    path.startsWith('/'),
                    `sse.subscription(path): path must start with "/" (got ${JSON.stringify(path)})`,
                );

                Joi.attempt(
                    subConfig,
                    subscriptionConfigSchema,
                    `Invalid @hapi/sse subscription config for "${path}":`,
                );

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

                        if (subConfig.refuse && (await subConfig.refuse(request))) {
                            request.raw.res.writeHead(204);
                            request.raw.res.end();

                            return h.abandon;
                        }

                        const lastEventId = request.headers['last-event-id'];
                        const incomingId = Array.isArray(lastEventId) ? lastEventId[0] : lastEventId;

                        if (incomingId && (await completionStore.get(incomingId))) {
                            await completionStore.drop(incomingId);
                            request.raw.res.writeHead(204);
                            request.raw.res.end();

                            return h.abandon;
                        }

                        const maxSessions = subConfig.maxSessions;

                        if (maxSessions && registry.subscriptionSessionCount(matched.pattern) >= maxSessions) {
                            return Boom.serverUnavailable('Too many connections');
                        }

                        const session = new Session({
                            request,
                            retry: clampRetry(subConfig.retry ?? config.retry),
                            keepAlive: subConfig.keepAlive ?? config.keepAlive,
                            headers: config.headers,
                            backpressure: options.backpressure,
                            maxDuration: subConfig.maxDuration,
                        });

                        if (subConfig.onSubscribe) {
                            await subConfig.onSubscribe(session, request.path, matched.params);
                        }

                        if (!session.isOpen) {
                            return h.abandon;
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

                        const replayer = subConfig.replay;

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

        server.decorate('handler', 'sse', (route: RequestRoute, handlerOptions: SseHandlerOptions) => {
            Joi.attempt(
                handlerOptions,
                handlerOptionsSchema,
                `Invalid @hapi/sse handler options for ${route.method.toUpperCase()} ${route.path}:`,
            );

            return async (request: Request, h: ResponseToolkit): Promise<Lifecycle.ReturnValue> => {
                const session = new Session({
                    request,
                    retry: clampRetry(handlerOptions.retry ?? config.retry),
                    keepAlive: handlerOptions.keepAlive ?? config.keepAlive,
                    headers: handlerOptions.headers ?? config.headers,
                    backpressure: handlerOptions.backpressure ?? options.backpressure,
                    maxDuration: handlerOptions.maxDuration,
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
