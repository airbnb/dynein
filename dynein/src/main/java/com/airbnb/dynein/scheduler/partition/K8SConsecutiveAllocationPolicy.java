/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See LICENSE in the project root for license
 * information.
 */
package com.airbnb.dynein.scheduler.partition;

import com.airbnb.dynein.scheduler.Constants;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.ReplicaSet;
import io.fabric8.kubernetes.client.AppsAPIGroupClient;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import java.text.Collator;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor = @__(@Inject))
public class K8SConsecutiveAllocationPolicy implements PartitionPolicy {
  private static final String K8S_CONDITION_READY = "Ready";
  private static final String K8S_CONDITION_TRUE = "True";
  private final DefaultKubernetesClient kubernetesClient;
  private final AppsAPIGroupClient appsClient;

  @Named(Constants.MAX_PARTITIONS)
  private final Integer numPartitions;

  public List<Integer> getPartitions() {
    String podName = System.getenv("K8S_POD_NAME");
    String namespace = System.getenv("K8S_NAMESPACE");
    NamespacedKubernetesClient namespacedClient = kubernetesClient.inNamespace(namespace);
    ReplicaSet replicaSet;

    Pod pod = namespacedClient.pods().withName(podName).get();
    String replicaSetName = pod.getMetadata().getOwnerReferences().get(0).getName();
    replicaSet = appsClient.replicaSets().inNamespace(namespace).withName(replicaSetName).get();
    FilterWatchListDeletable<Pod, PodList, Boolean, Watch, Watcher<Pod>> deploymentPods =
        namespacedClient.pods().withLabelSelector(replicaSet.getSpec().getSelector());

    List<String> activePods =
        deploymentPods
            .list()
            .getItems()
            .stream()
            .filter(
                isActive ->
                    isActive
                        .getStatus()
                        .getConditions()
                        .stream()
                        .anyMatch(
                            condition ->
                                condition.getType().equals(K8S_CONDITION_READY)
                                    && condition.getStatus().equals(K8S_CONDITION_TRUE)))
            .map(it -> it.getMetadata().getName())
            .sorted(Collator.getInstance())
            .collect(Collectors.toList());

    int podIndex = activePods.indexOf(podName);
    int numPods = activePods.size();

    if (numPods == 0 || podIndex == -1) {
      return new ArrayList<>();
    }
    List<Integer> partitions = new ArrayList<>();
    int split = numPartitions / numPods;
    int start = podIndex * split;
    int end = (podIndex == numPods - 1) ? numPartitions - 1 : ((podIndex + 1) * split) - 1;
    for (int i = start; i <= end; i++) {
      partitions.add(i);
    }
    return partitions;
  }
}
