package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.controller.cluster.ClusterController;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.resources.AbstractCluster;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractClusterOperations<C extends AbstractCluster, R extends HasMetadata> {

    private static final Logger log = LoggerFactory.getLogger(AbstractClusterOperations.class.getName());

    protected static final String OP_CREATE = "create";
    protected static final String OP_DELETE = "delete";
    protected static final String OP_UPDATE = "update";

    protected static final int LOCK_TIMEOUT = 60000;

    protected final Vertx vertx;
    protected final boolean isOpenShift;
    protected final String clusterDescription;
    protected final ConfigMapOperations configMapOperations;

    // super(vertx, isOpenShift, "Kafka", configMapOperations);
    // super(vertx, isOpenShift, "Kafka Connect", configMapOperations);
    // super(vertx, isOpenShift, "Kafka Connect S2I", configMapOperations);
    protected AbstractClusterOperations(
        Vertx vertx,
        boolean isOpenShift,
        String clusterDescription,
        ConfigMapOperations configMapOperations
    ) {
        this.vertx = vertx;
        this.isOpenShift = isOpenShift;
        this.clusterDescription = clusterDescription;
        this.configMapOperations = configMapOperations;
    }

    protected final String getLockName(String clusterType, String namespace, String name) {
        return "lock::" + clusterType + "::" + namespace + "::" + name;
    }

    // import io.strimzi.controller.cluster.resources.AbstractCluster;
    // import io.strimzi.controller.cluster.resources.ClusterDiffResult;
    // new ClusterOperation<>(KafkaCluster.fromConfigMap(configMapOperations.get(namespace, name)), null);
    protected static class ClusterOperation<C extends AbstractCluster> {
        private final C cluster;
        private final ClusterDiffResult diff;

        public ClusterOperation(C cluster, ClusterDiffResult diff) {
            this.cluster = cluster;
            this.diff = diff;
        }

        public C cluster() {
            return cluster;
        }

        public ClusterDiffResult diff() {
            return diff;
        }

    }

    // import io.strimzi.controller.cluster.resources.AbstractCluster;
    // new CompositeOperation<KafkaCluster>()
    // new CompositeOperation<ZookeeperCluster>()
    // new CompositeOperation<TopicController>()
    protected interface CompositeOperation<C extends AbstractCluster> {
        String operationType();
        String clusterType();
        Future<?> composite(String namespace, ClusterOperation<C> operation);
        ClusterOperation<C> getCluster(String namespace, String name);
    }

    // import io.strimzi.controller.cluster.resources.AbstractCluster;
    protected final <C extends AbstractCluster> void execute(
        String namespace,
        String name,
        CompositeOperation<C> compositeOperation,
        Handler<AsyncResult<Void>> handler
    ) {
        String clusterType = compositeOperation.clusterType();
        String operationType = compositeOperation.operationType();
        ClusterOperation<C> clusterOp;
        try {
            clusterOp = compositeOperation.getCluster(namespace, name);
            log.info("{} {} cluster {} in namespace {}", operationType, clusterType, clusterOp.cluster().getName(), namespace);
        } catch (Throwable ex) {
            log.error("Error while getting required {} cluster state for {} operation", clusterType, operationType, ex);
            handler.handle(Future.failedFuture("getCluster error"));
            return;
        }
        Future<?> composite = compositeOperation.composite(namespace, clusterOp);

        composite.setHandler(ar -> {
            if (ar.succeeded()) {
                log.info("{} cluster {} in namespace {}: successful {}", clusterType, clusterOp.cluster().getName(), namespace, operationType);
                handler.handle(Future.succeededFuture());
            } else {
                log.error("{} cluster {} in namespace {}: failed to {}", clusterType, clusterOp.cluster().getName(), namespace, operationType);
                handler.handle(Future.failedFuture("Failed to execute cluster operation"));
            }
        });
    }

    protected abstract void create(String namespace, String name, Handler<AsyncResult<Void>> handler);

    protected abstract void delete(String namespace, String name, Handler<AsyncResult<Void>> handler);

    protected abstract void update(String namespace, String name, Handler<AsyncResult<Void>> handler);

    // ConfigMap cm = configMapOperations.get(namespace, name);
    // name(cm);
    protected String name(HasMetadata resource) {
        return resource.getMetadata().getName();
    }

    // statefulSet
    // List<R> resources = getResources(namespace, labels);
    // nameFromLabels(resource);
    // // STRIMZI_CLUSTER_LABEL = "strimzi.io/cluster";
    protected String nameFromLabels(R resource) {
        return resource.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL);
    }

    protected abstract String clusterType();

    public final void reconcile(String namespace, String name) {
        // kafka
        // kafka-connect
        // kafka-connect-s2i
        String clusterType = clusterType();

        // lock::kafka::namespace::name
        // lock::kafka-connect::namespace::name
        // lock::kafka-connect-s2i::namespace::name
        final String lockName = getLockName(clusterType, namespace, name);

        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                log.debug("Lock {} acquired", lockName);
                Lock lock = res.result();

                try {
                    log.info("Reconciling {} clusters ...", clusterDescription);

                    ConfigMap cm = configMapOperations.get(namespace, name);

                    Map<String, String> labels = new HashMap<>();
                    // STRIMZI_CLUSTER_LABEL = "strimzi.io/cluster";
                    labels.put(ClusterController.STRIMZI_CLUSTER_LABEL, name);

                    // statefulSet
                    List<R> resources = getResources(namespace, labels);

                    if (cm != null) {
                        String nameFromCm = name(cm);
                        if (resources.size() > 0) {
                            log.info("Reconciliation: {} cluster {} should be checked for updates", clusterDescription, cm.getMetadata().getName());
                            log.info("Checking for updates in {} cluster {}", clusterDescription, nameFromCm);
                            update(namespace, nameFromCm, updateResult -> {
                                if (updateResult.succeeded()) {
                                    log.info("{} cluster updated {}", clusterDescription, nameFromCm);
                                } else {
                                    log.error("Failed to update {} cluster {}.", clusterDescription, nameFromCm);
                                }
                                lock.release();
                                log.debug("Lock {} released", lockName);
                            });
                        } else {
                            log.info("Reconciliation: {} cluster {} should be created", clusterDescription, cm.getMetadata().getName());
                            log.info("Adding {} cluster {}", clusterDescription, nameFromCm);
                            create(namespace, nameFromCm, createResult -> {
                                if (createResult.succeeded()) {
                                    log.info("{} cluster added {}", clusterDescription, nameFromCm);
                                } else {
                                    log.error("Failed to add {} cluster {}.", clusterDescription, nameFromCm);
                                }
                                lock.release();
                                log.debug("Lock {} released", lockName);
                            });
                        }
                    } else {
                        List<Future> result = new ArrayList<>(resources.size());
                        for (R resource : resources) {
                            log.info("Reconciliation: {} cluster {} should be deleted", clusterDescription, resource.getMetadata().getName());
                            String nameFromResource = nameFromLabels(resource);
                            log.info("Deleting {} cluster {} in namespace {}", clusterDescription, nameFromResource, namespace);

                            Future<Void> deleteFuture = Future.future();
                            result.add(deleteFuture);
                            delete(namespace, nameFromResource, deleteResult -> {
                                if (deleteResult.succeeded()) {
                                    log.info("{} cluster deleted {} in namespace {}", clusterDescription, nameFromResource, namespace);
                                } else {
                                    log.error("Failed to delete {} cluster {} in namespace {}", clusterDescription, nameFromResource, namespace);
                                }
                                deleteFuture.complete();
                            });
                        }

                        CompositeFuture.join(result).setHandler(res2 -> {
                            lock.release();
                            log.debug("Lock {} released", lockName);
                        });
                    }
                } catch (Throwable ex) {
                    log.error("Error while reconciling {} cluster", clusterDescription, ex);
                    lock.release();
                    log.debug("Lock {} released", lockName);
                }

            } else {
                log.warn("Failed to acquire lock for {} cluster {}.", clusterType, lockName);
            }
        });
    }












    public final void reconcileAll(String namespace, Map<String, String> labels) {
        String clusterType = clusterType();
        Map<String, String> newLabels = new HashMap<>(labels);
        newLabels.put(ClusterController.STRIMZI_TYPE_LABEL, clusterType);

        // get ConfigMap for the corresponding cluster type
        List<ConfigMap> cms = configMapOperations.list(namespace, newLabels);
        Set<String> cmsNames = cms.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toSet());

        // get resources for the corresponding cluster name (they are part of)
        List<R> resources = getResources(namespace, newLabels);
        Set<String> resourceNames = resources.stream().map(res -> res.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL)).collect(Collectors.toSet());

        cmsNames.addAll(resourceNames);

        for (String name: cmsNames) {
            reconcile(namespace, name);
        }
    }

    protected abstract List<R> getResources(String namespace, Map<String, String> kafkaLabels);

}
