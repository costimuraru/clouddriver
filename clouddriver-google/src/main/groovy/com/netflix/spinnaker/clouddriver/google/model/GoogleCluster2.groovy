/*
 * Copyright 2014 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.clouddriver.google.model

import com.fasterxml.jackson.annotation.JsonIgnore
import com.netflix.spinnaker.clouddriver.google.GoogleCloudProvider
import com.netflix.spinnaker.clouddriver.model.Cluster
import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode

@CompileStatic
@EqualsAndHashCode(includes = ["name", "accountName"])
class GoogleCluster2 {
  String name
  String accountName

  @JsonIgnore
  View getView() {
    new View()
  }

  @Canonical
  class View implements Cluster {

    final String type = GoogleCloudProvider.GCE

    String name = GoogleCluster2.this.name
    String accountName = GoogleCluster2.this.accountName

    Set<GoogleServerGroup2.View> serverGroups = Collections.synchronizedSet([] as Set)
    Set<GoogleLoadBalancer2.View> loadBalancers = Collections.synchronizedSet([] as Set)
  }
}
