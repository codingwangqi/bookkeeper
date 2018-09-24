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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * A service builder that overrides streaming methods with efficient implementation.
 */
@Slf4j
public final class StreamingServiceBuilder {

    /**
     * Create a streaming service builder to binding streaming methods in existing <tt>service</tt>.
     *
     * @param service service to bind streaming methods
     * @return streaming service builder
     */
    public static StreamingServiceBuilder builder(BindableService service) {
        return new StreamingServiceBuilder(service);
    }

    private final BindableService service;
    private final Map<String, ReadStream<ChunkMessage, ChunkMessage>> readMethods =
        new HashMap<>();
    private final Map<String, WriteStream<ChunkMessage, ChunkMessage>> writeMethods =
        new HashMap<>();

    private StreamingServiceBuilder(BindableService service) {
        this.service = service;
    }

    /**
     * Register a read method.
     *
     * @param methodName method name
     * @param readStream read stream
     * @return streaming service builder
     */
    public StreamingServiceBuilder addReadMethod(String methodName,
                                                 ReadStream<ChunkMessage, ChunkMessage> readStream) {
        readMethods.put(methodName, readStream);
        return this;
    }

    /**
     * Register a write method.
     *
     * @param methodName method name
     * @param writeStream write stream
     * @return streaming service builder
     */
    public StreamingServiceBuilder addWriteMethod(String methodName,
                                                  WriteStream<ChunkMessage, ChunkMessage> writeStream) {
        writeMethods.put(methodName, writeStream);
        return this;
    }

    /**
     * Build the new bindable service.
     *
     * @return the new service with new streaming methods
     */
    public BindableService build() {
        Set<String> readMethodNames = readMethods.keySet();
        Set<String> writeMethodNames = writeMethods.keySet();
        checkArgument(
            Sets.intersection(readMethodNames, writeMethodNames).isEmpty(),
            "Write methods and read methods have overlaps : write methods = "
                + writeMethodNames + ", read methods = " + readMethodNames);

        ServerServiceDefinition definition = service.bindService();
        String serviceName = definition.getServiceDescriptor().getName();

        ImmutableList.Builder<DataChunkStream<?, ?>> streams = ImmutableList.builder();
        writeMethods.forEach((methodName, writeStream) -> streams.add(new DataChunkWriteStream<>(
            serviceName,
            methodName,
            ChunkMessage.createMarshaller(PooledByteBufAllocator.DEFAULT),
            ChunkMessage.createMarshaller(PooledByteBufAllocator.DEFAULT),
            writeStream
        )));
        readMethods.forEach((methodName, readStream) -> streams.add(new DataChunkReadStream<>(
            serviceName,
            methodName,
            ChunkMessage.createMarshaller(PooledByteBufAllocator.DEFAULT),
            ChunkMessage.createMarshaller(PooledByteBufAllocator.DEFAULT),
            readStream
        )));
        return new DataChunkBindingService(
            () -> definition,
            streams.build()
        );
    }

}
