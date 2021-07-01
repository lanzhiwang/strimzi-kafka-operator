package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.DoneableEndpoints;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsList;
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
public class EndpointOperations extends AbstractOperations<KubernetesClient, Endpoints, EndpointsList, DoneableEndpoints, Resource<Endpoints, DoneableEndpoints>> {

    public EndpointOperations(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Endpoints");
    }

    @Override
    protected MixedOperation<Endpoints, EndpointsList, DoneableEndpoints, Resource<Endpoints, DoneableEndpoints>> operation() {
        return client.endpoints();
    }
}
