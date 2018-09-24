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

import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition.Builder;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.ServerCalls.ServerStreamingMethod;
import io.grpc.stub.StreamObserver;

/**
 * Read a stream of chunks.
 */
class DataChunkReadStream<RequestT, ChunkT> implements DataChunkStream<RequestT, ChunkT> {

    static class ReadMethod<RequestT, ChunkT> implements ServerStreamingMethod<RequestT, ChunkT> {

        private final ReadStream<RequestT, ChunkT> stream;

        ReadMethod(ReadStream<RequestT, ChunkT> stream) {
            this.stream = stream;
        }

        @Override
        public void invoke(RequestT request, StreamObserver<ChunkT> streamObserver) {
            stream.read(request, streamObserver);
        }
    }

    private final String fullMethodName;
    private final MethodDescriptor<RequestT, ChunkT> methodDescriptor;
    private final ReadStream<RequestT, ChunkT> readStream;

    DataChunkReadStream(String serviceName,
                        String methodName,
                        Marshaller<RequestT> reqMarshaller,
                        Marshaller<ChunkT> respMarshaller,
                        ReadStream<RequestT, ChunkT> readStream) {
        this.fullMethodName = MethodDescriptor.generateFullMethodName(serviceName, methodName);
        this.methodDescriptor = MethodDescriptor.<RequestT, ChunkT>newBuilder()
            .setType(MethodType.SERVER_STREAMING)
            .setFullMethodName(fullMethodName)
            .setSampledToLocalTracing(false)
            .setRequestMarshaller(reqMarshaller)
            .setResponseMarshaller(respMarshaller)
            .build();
        this.readStream = readStream;
    }

    @Override
    public MethodDescriptor<RequestT, ChunkT> getMethodDescriptor() {
        return methodDescriptor;
    }

    @Override
    public ServerCallHandler<RequestT, ChunkT> getServerCallHandler() {
        return ServerCalls.asyncServerStreamingCall(
            new ReadMethod<>(readStream)
        );
    }

    @Override
    public void addMethodToService(Builder serviceBuilder) {
        serviceBuilder.addMethod(methodDescriptor, getServerCallHandler());
    }
}
