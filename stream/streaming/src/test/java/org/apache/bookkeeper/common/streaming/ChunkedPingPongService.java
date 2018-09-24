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

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.tests.rpc.PingPongService;
import org.bookkeeper.tests.proto.rpc.PingRequest;
import org.bookkeeper.tests.proto.rpc.PongResponse;

/**
 * Ping-Pong service with chunk messages.
 */
@Slf4j
class ChunkedPingPongService {

    private final int streamPongSize;
    private final PingPongService baseService;

    ChunkedPingPongService(int streamPongSize) {
        this.streamPongSize = streamPongSize;
        this.baseService = new PingPongService(streamPongSize);
    }

    public ServerServiceDefinition bindService() {
        BindableService streamingService = StreamingServiceBuilder.builder(baseService)
            .addReadMethod("LotsOfChunkPongs", this::lotsOfChunkPongs)
            .addWriteMethod("LotsOfChunkPings", this::lotsOfChunkPings)
            .build();
        return streamingService.bindService();
    }

    public void lotsOfChunkPongs(ChunkMessage request,
                                 StreamObserver<ChunkMessage> respObserver) {
        PingRequest requestHeader;
        PingRequest requestPayload;
        try {
            requestHeader = PingRequest.parseFrom(request.getHeader().nioBuffer());
            requestPayload = PingRequest.parseFrom(request.getPayload().nioBuffer());
        } catch (InvalidProtocolBufferException e) {
            log.error("Received invalid ping request : ", e);
            respObserver.onError(new StatusRuntimeException(Status.INTERNAL));
            return;
        }

        if (!requestHeader.equals(requestPayload)) {
            log.error("Receive unmatched header and payload : header = {}, payload = {}",
                requestHeader, requestPayload);
            respObserver.onError(new StatusRuntimeException(Status.INTERNAL));
            return;
        }

        long sequence = requestHeader.getSequence();
        for (int i = 0; i < streamPongSize; i++) {
            PongResponse resp = PongResponse.newBuilder()
                .setNumPingReceived(1)
                .setLastSequence(sequence)
                .setSlotId(i)
                .build();
            try (ChunkMessage message = new ChunkMessage(
                Unpooled.wrappedBuffer(resp.toByteArray()),
                Unpooled.wrappedBuffer(resp.toByteArray())
            )) {
                respObserver.onNext(message);
            }
        }
        respObserver.onCompleted();
    }

    public StreamObserver<ChunkMessage> lotsOfChunkPings(StreamObserver<ChunkMessage> respObserver) {
        return new StreamObserver<ChunkMessage>() {

            int pingCount = 0;
            long lastSequence = -1L;

            @Override
            public void onNext(ChunkMessage chunkMessage) {
                try {
                    PingRequest pingHeader;
                    PingRequest pingPayload;
                    try {
                        pingHeader = PingRequest.parseFrom(chunkMessage.getHeader().nioBuffer());
                        log.info("Received ping request header : {}", pingHeader);
                        pingPayload = PingRequest.parseFrom(chunkMessage.getPayload().nioBuffer());
                        log.info("Received ping request payload : {}", pingPayload);
                    } catch (InvalidProtocolBufferException e) {
                        log.error("Received invalid ping request : ", e);
                        respObserver.onError(new StatusRuntimeException(
                            Status.INTERNAL
                        ));
                        return;
                    }

                    if (!pingHeader.equals(pingPayload)) {
                        log.error("Receive unmatched header and payload : header = {}, payload = {}",
                            pingHeader, pingPayload);
                        respObserver.onError(new StatusRuntimeException(
                            Status.INTERNAL
                        ));
                        return;
                    }

                    pingCount++;
                    lastSequence = pingHeader.getSequence();
                } finally {
                    chunkMessage.close();
                }
            }

            @Override
            public void onError(Throwable cause) {
                log.error("Failed on receiving stream of pings", cause);
            }

            @Override
            public void onCompleted() {
                PongResponse resp = PongResponse.newBuilder()
                    .setNumPingReceived(pingCount)
                    .setLastSequence(lastSequence)
                    .setSlotId(0)
                    .build();
                try (ChunkMessage message = new ChunkMessage(
                    Unpooled.wrappedBuffer(resp.toByteArray()),
                    Unpooled.wrappedBuffer(resp.toByteArray())
                )) {
                    respObserver.onNext(message);
                }
                respObserver.onCompleted();
            }
        };
    }
}
