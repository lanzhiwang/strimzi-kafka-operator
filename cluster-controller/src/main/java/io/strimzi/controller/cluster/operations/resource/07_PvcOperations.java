package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.DoneablePersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

// public abstract class AbstractOperations<
//     C,
//     T extends HasMetadata,
//     L extends KubernetesResourceList/*<T>*/,
//     D,
//     R extends Resource<T, D>>
public class PvcOperations extends AbstractOperations<KubernetesClient, PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> {

    public PvcOperations(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "PersistentVolumeClaim");
    }

    @Override
    protected MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> operation() {
        return client.persistentVolumeClaims();
    }

    @Override
    public Future<Void> create(PersistentVolumeClaim resource) {
        throw new UnsupportedOperationException(); // should never happen
    }

    @Override
    public Future<Void> patch(String namespace, String name, PersistentVolumeClaim patch) {
        throw new UnsupportedOperationException(); // should never happen
    }
}
