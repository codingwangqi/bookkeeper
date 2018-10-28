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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

/**
 * Data chunk client that talks in data chunks.
 */
public class ReadStreamClient<RequestT, ResponseT> implements ReadStream<RequestT, ResponseT> {

    private final Channel channel;
    private final MethodDescriptor<RequestT, ResponseT> descriptor;

    ReadStreamClient(Channel channel, MethodDescriptor<RequestT, ResponseT> descriptor) {
        this.channel = channel;
        this.descriptor = descriptor;
    }

    @Override
    public void read(RequestT request, StreamObserver<ResponseT> respObserver) {
        ClientCall<RequestT, ResponseT> call = channel.newCall(
            descriptor,
            CallOptions.DEFAULT
        );
        ClientCalls.asyncServerStreamingCall(call, request, respObserver);
    }
}
