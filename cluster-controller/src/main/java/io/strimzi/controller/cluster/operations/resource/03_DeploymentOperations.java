package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentList;
import io.fabric8.kubernetes.api.model.extensions.DoneableDeployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.vertx.core.Vertx;

// public abstract class AbstractOperations<
//     C,
//     T extends HasMetadata,
//     L extends KubernetesResourceList/*<T>*/,
//     D,
//     R extends Resource<T, D>>

// public abstract class AbstractScalableOperations<
//     C,
//     T extends HasMetadata,
//     L extends KubernetesResourceList/*<T>*/,
//     D,
//     R extends ScalableResource<T, D> > extends AbstractOperations<C, T, L, D, R>
public class DeploymentOperations extends AbstractScalableOperations<
    KubernetesClient,
    Deployment,
    DeploymentList,
    DoneableDeployment,
    ScalableResource<Deployment, DoneableDeployment>> {

    public DeploymentOperations(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Deployment");
    }

    // protected abstract MixedOperation<T, L, D, R> operation();
    @Override
    protected MixedOperation<Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> operation() {
        return client.extensions().deployments();
    }
}
