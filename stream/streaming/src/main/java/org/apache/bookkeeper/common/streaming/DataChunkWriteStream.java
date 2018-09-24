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
import io.grpc.stub.ServerCalls.ClientStreamingMethod;
import io.grpc.stub.StreamObserver;

/**
 * Write a stream of chunk.
 */
class DataChunkWriteStream<ChunkT, ResultT> implements DataChunkStream<ChunkT, ResultT> {

    static class WriteMethod<ChunkT, ResultT> implements ClientStreamingMethod<ChunkT, ResultT> {

        private final WriteStream<ChunkT, ResultT> stream;

        WriteMethod(WriteStream<ChunkT, ResultT> stream) {
            this.stream = stream;
        }

        @Override
        public StreamObserver<ChunkT> invoke(StreamObserver<ResultT> streamObserver) {
            return stream.write(streamObserver);
        }
    }

    private final String fullMethodName;
    private final MethodDescriptor<ChunkT, ResultT> methodDescriptor;
    private final WriteStream<ChunkT, ResultT> writeStream;

    DataChunkWriteStream(String serviceName,
                         String methodName,
                         Marshaller<ChunkT> reqMarshaller,
                         Marshaller<ResultT> respMarshaller,
                         WriteStream<ChunkT, ResultT> writeStream) {
        this.fullMethodName = MethodDescriptor.generateFullMethodName(serviceName, methodName);
        this.methodDescriptor = MethodDescriptor.<ChunkT, ResultT>newBuilder()
            .setType(MethodType.CLIENT_STREAMING)
            .setFullMethodName(fullMethodName)
            .setSampledToLocalTracing(false)
            .setRequestMarshaller(reqMarshaller)
            .setResponseMarshaller(respMarshaller)
            .build();
        this.writeStream = writeStream;
    }

    @Override
    public MethodDescriptor<ChunkT, ResultT> getMethodDescriptor() {
        return methodDescriptor;
    }

    @Override
    public ServerCallHandler<ChunkT, ResultT> getServerCallHandler() {
        return ServerCalls.asyncClientStreamingCall(
            new WriteMethod<>(writeStream)
        );
    }

    @Override
    public void addMethodToService(Builder serviceBuilder) {
        serviceBuilder.addMethod(methodDescriptor, getServerCallHandler());
    }
}
