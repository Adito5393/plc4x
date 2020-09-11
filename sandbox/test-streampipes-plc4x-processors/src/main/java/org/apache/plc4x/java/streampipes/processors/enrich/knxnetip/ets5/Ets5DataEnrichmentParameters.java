/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.plc4x.java.streampipes.processors.enrich.knxnetip.ets5;

import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class Ets5DataEnrichmentParameters extends EventProcessorBindingParams {

    private final String destinationIdFieldName;
    private final String payloadIdFieldName;

    public Ets5DataEnrichmentParameters(DataProcessorInvocation graph, String destinationIdFieldName, String payloadIdFieldName) {
        super(graph);
        this.destinationIdFieldName = destinationIdFieldName;
        this.payloadIdFieldName = payloadIdFieldName;
    }

    public String getDestinationIdFieldName() {
        return destinationIdFieldName;
    }

    public String getPayloadIdFieldName() {
        return payloadIdFieldName;
    }

}
