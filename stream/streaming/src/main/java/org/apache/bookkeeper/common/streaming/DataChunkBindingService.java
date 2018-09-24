/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.common.streaming;

import com.google.common.collect.Sets;
import io.grpc.BindableService;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Extends an existing service to override some methods for more efficient streaming implementations.
 */
class DataChunkBindingService implements BindableService {

    private final Supplier<ServerServiceDefinition> serverServiceDefinitionSupplier;
    private final Collection<DataChunkStream<?, ?>> streamMethods;

    DataChunkBindingService(Supplier<ServerServiceDefinition> serverServiceDefinitionSupplier,
                            Collection<DataChunkStream<?, ?>> streamMethods) {
        this.serverServiceDefinitionSupplier = serverServiceDefinitionSupplier;
        this.streamMethods = streamMethods;
    }

    @Override
    public ServerServiceDefinition bindService() {
        final ServerServiceDefinition delegatedServiceDefinition = serverServiceDefinitionSupplier.get();

        ServerServiceDefinition.Builder serviceBuilder =
            ServerServiceDefinition.builder(delegatedServiceDefinition.getServiceDescriptor().getName());

        Set<String> overrideMethods = Sets.newHashSet();

        streamMethods.forEach(stream -> {
            stream.addMethodToService(serviceBuilder);
            overrideMethods.add(stream.getMethodDescriptor().getFullMethodName());
        });

        // copy over non-overridden methods
        for (ServerMethodDefinition<?, ?> definition : delegatedServiceDefinition.getMethods()) {
            if (overrideMethods.contains(definition.getMethodDescriptor().getFullMethodName())) {
                continue;
            }
            serviceBuilder.addMethod(definition);
        }

        return serviceBuilder.build();
    }

}
