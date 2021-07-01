package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.internal.readiness.Readiness;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public abstract class AbstractOperations<C, T extends HasMetadata, L extends KubernetesResourceList/*<T>*/, D, R extends Resource<T, D>> {

    private static final Logger log = LoggerFactory.getLogger(AbstractOperations.class);
    protected final Vertx vertx;
    protected final C client;
    private final String resourceKind;

    public AbstractOperations(Vertx vertx, C client, String resourceKind) {
        this.vertx = vertx;
        this.client = client;
        this.resourceKind = resourceKind;
    }

    protected abstract MixedOperation<T, L, D, R> operation();

    @SuppressWarnings("unchecked")
    public Future<Void> create(T resource) {
        Future<Void> fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                String namespace = resource.getMetadata().getNamespace();
                String name = resource.getMetadata().getName();
                if (operation().inNamespace(namespace).withName(name).get() == null) {
                    try {
                        log.info("Creating {} {} in namespace {}", resourceKind, name, namespace);
                        operation().inNamespace(namespace).createOrReplace(resource);
                        log.info("{} {} in namespace {} has been created", resourceKind, name, namespace);
                        future.complete();
                    } catch (Exception e) {
                        log.error("Caught exception while creating {} {} in namespace {}", resourceKind, name, namespace, e);
                        future.fail(e);
                    }
                } else {
                    log.warn("{} {} in namespace {} already exists", resourceKind, name, namespace);
                    future.complete();
                }
            },
            false,
            fut.completer()
        );
        return fut;
    }

    public Future<Void> delete(String namespace, String name) {
        Future<Void> fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                if (operation().inNamespace(namespace).withName(name).get() != null) {
                    try {
                        log.info("Deleting {} {} in namespace {}", resourceKind, name, namespace);
                        operation().inNamespace(namespace).withName(name).delete();
                        log.info("{} {} in namespace {} has been deleted", resourceKind, name, namespace);
                        future.complete();
                    } catch (Exception e) {
                        log.error("Caught exception while deleting {} {} in namespace {}", resourceKind, name, namespace, e);
                        future.fail(e);
                    }
                } else {
                    log.warn("{} {} in namespace {} doesn't exist, so cannot be deleted", resourceKind, name, namespace);
                    future.complete();
                }
            }, false,
            fut.completer()
        );
        return fut;
    }

    public Future<Void> patch(String namespace, String name, T patch) {
        return patch(namespace, name, true, patch);
    }

    public Future<Void> patch(String namespace, String name, boolean cascading, T patch) {
        Future<Void> fut = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    log.info("Patching {} resource {} in namespace {} with {}", resourceKind, name, namespace, patch);
                    operation().inNamespace(namespace).withName(name).cascading(cascading).patch(patch);
                    log.info("{} {} in namespace {} has been patched", resourceKind, name, namespace);
                    future.complete();
                } catch (Exception e) {
                    log.error("Caught exception while patching {} {} in namespace {}", resourceKind, name, namespace, e);
                    future.fail(e);
                }
            },
            true,
            fut.completer()
        );
        return fut;
    }

    public T get(String namespace, String name) {
        return operation().inNamespace(namespace).withName(name).get();
    }

    @SuppressWarnings("unchecked")
    public List<T> list(String namespace, Map<String, String> labels) {
        return operation().inNamespace(namespace).withLabels(labels).list().getItems();
    }

    public Future<Void> readiness(String namespace, String name, long pollIntervalMs, long timeoutMs) {
        Future<Void> fut = Future.future();
        log.info("Waiting for {} resource {} in namespace {} to get ready", resourceKind, name, namespace);
        long deadline = System.currentTimeMillis() + timeoutMs;

        Handler<Long> handler = new Handler<Long>() {
            @Override
            public void handle(Long timerId) {

                vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                    future -> {
                        try {
                            if (isReady(namespace, name))   {
                                future.complete();
                            } else {
                                if (log.isTraceEnabled()) {
                                    log.trace("{} {} in namespace {} is not ready", resourceKind, name, namespace);
                                }
                                future.fail("Not ready yet");
                            }
                        } catch (Exception e) {
                            log.warn("Caught exception while waiting for {} {} in namespace {} to get ready", resourceKind, name, namespace, e);
                            future.fail(e);
                        }
                    },
                    false,
                    res -> {
                        if (res.succeeded()) {
                            log.info("{} {} in namespace {} is ready", resourceKind, name, namespace);
                            fut.complete();
                        } else {
                            long timeLeft = deadline - System.currentTimeMillis();
                            if (timeLeft <= 0) {
                                log.error("Exceeded timeoutMs of {} ms while waiting for {} {} in namespace {} to be ready", timeoutMs, resourceKind, name, namespace);
                                fut.fail(new TimeoutException());
                            } else {
                                // Schedule ourselves to run again
                                vertx.setTimer(Math.min(pollIntervalMs, timeLeft), this);
                            }
                        }
                    }
                );
            }
        };

        // Call the handler ourselves the first time
        handler.handle(null);

        return fut;
    }

    public boolean isReady(String namespace, String name) {
        R resourceOp = operation().inNamespace(namespace).withName(name);
        T resource = resourceOp.get();
        if (resource != null)   {
            if (Readiness.isReadinessApplicable(resource)) {
                return Boolean.TRUE.equals(resourceOp.isReady());
            } else {
                return true;
            }
        } else {
            return false;
        }
    }
}
