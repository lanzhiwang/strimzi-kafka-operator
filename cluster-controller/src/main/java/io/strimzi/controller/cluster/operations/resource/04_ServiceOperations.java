package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

// public abstract class AbstractOperations<
//     C,
//     T extends HasMetadata,
//     L extends KubernetesResourceList/*<T>*/,
//     D,
//     R extends Resource<T, D>>
public class ServiceOperations extends AbstractOperations<KubernetesClient, Service, ServiceList, DoneableService, Resource<Service, DoneableService>> {
    public ServiceOperations(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Service");
    }

    @Override
    protected MixedOperation<Service, ServiceList, DoneableService, Resource<Service, DoneableService>> operation() {
        return client.services();
    }
}
