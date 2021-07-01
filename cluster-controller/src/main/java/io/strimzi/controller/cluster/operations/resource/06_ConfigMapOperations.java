package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
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
public class ConfigMapOperations extends AbstractOperations<KubernetesClient, ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> {

    public ConfigMapOperations(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "ConfigMap");
    }

    @Override
    protected MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> operation() {
        return client.configMaps();
    }
}
