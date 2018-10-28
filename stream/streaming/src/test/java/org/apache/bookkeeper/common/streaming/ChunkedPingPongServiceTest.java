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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.bookkeeper.tests.proto.rpc.ChunkPingRequest;
import org.bookkeeper.tests.proto.rpc.ChunkPongResponse;
import org.bookkeeper.tests.proto.rpc.PingPongServiceGrpc;
import org.bookkeeper.tests.proto.rpc.PingPongServiceGrpc.PingPongServiceBlockingStub;
import org.bookkeeper.tests.proto.rpc.PingPongServiceGrpc.PingPongServiceStub;
import org.bookkeeper.tests.proto.rpc.PingRequest;
import org.bookkeeper.tests.proto.rpc.PongResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test {@link ChunkedPingPongService}.
 */
public class ChunkedPingPongServiceTest {

    private static final int NUM_PONGS_PER_PING = 10;
    private static final String SERVICE_NAME = "chunkpingpong";

    private Server grpcServer;
    private ChunkedPingPongService service;
    private ManagedChannel clientChannel;
    private PingPongServiceStub client;

    @Before
    public void setup() throws Exception {
        this.service = new ChunkedPingPongService(NUM_PONGS_PER_PING);
        MutableHandlerRegistry registry = new MutableHandlerRegistry();
        this.grpcServer = ServerBuilder
            .forPort(0)
            .fallbackHandlerRegistry(registry)
            .build();
        this.grpcServer.start();
        registry.addService(this.service.bindService());
        this.clientChannel = ManagedChannelBuilder
            .forAddress("127.0.0.1", grpcServer.getPort())
            .usePlaintext()
            .build();
        this.client = PingPongServiceGrpc.newStub(clientChannel);
    }

    @After
    public void teardown() throws Exception {
        if (null != clientChannel) {
            clientChannel.shutdown();
        }
        if (null != grpcServer) {
            grpcServer.shutdown();
        }
    }

    @Test
    public void testServerChunkStreaming() throws Exception {
        PingPongServiceBlockingStub clientBlocking = PingPongServiceGrpc.newBlockingStub(clientChannel);

        long sequence = ThreadLocalRandom.current().nextLong(100000);
        PingRequest request = PingRequest.newBuilder()
            .setSequence(sequence)
            .build();
        ChunkPingRequest chunkRequest = ChunkPingRequest.newBuilder()
            .setHeader(ByteString.copyFrom(request.toByteArray()))
            .setPayload(ByteString.copyFrom(request.toByteArray()))
            .build();
        Iterator<ChunkPongResponse> respIter = clientBlocking.lotsOfChunkPongs(chunkRequest);
        int count = 0;
        while (respIter.hasNext()) {
            ChunkPongResponse resp = respIter.next();
            PongResponse header = PongResponse.parseFrom(resp.getHeader().asReadOnlyByteBuffer());
            PongResponse payload = PongResponse.parseFrom(resp.getPayload().asReadOnlyByteBuffer());
            assertEquals(header, payload);
            assertEquals(sequence, header.getLastSequence());
            assertEquals(1, header.getNumPingReceived());
            assertEquals(count, header.getSlotId());
            ++count;
        }
    }

    @Test
    public void testClientStreaming() throws Exception {
        final int numPings = 100;
        final long sequence = ThreadLocalRandom.current().nextLong(100000);
        final CompletableFuture<Void> respFuture = new CompletableFuture<>();
        final LinkedBlockingQueue<ChunkPongResponse> respQueue = new LinkedBlockingQueue<>();
        StreamObserver<ChunkPingRequest> pinger = client.lotsOfChunkPings(new StreamObserver<ChunkPongResponse>() {
            @Override
            public void onNext(ChunkPongResponse resp) {
                respQueue.offer(resp);
            }

            @Override
            public void onError(Throwable t) {
                respFuture.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                FutureUtils.complete(respFuture, null);
            }
        });

        for (int i = 0; i < numPings; i++) {
            PingRequest ping = PingRequest.newBuilder()
                .setSequence(sequence + i)
                .build();
            ChunkPingRequest request = ChunkPingRequest.newBuilder()
                .setHeader(ByteString.copyFrom(ping.toByteArray()))
                .setPayload(ByteString.copyFrom(ping.toByteArray()))
                .build();
            pinger.onNext(request);
        }
        pinger.onCompleted();

        // wait for response to be received.
        result(respFuture);

        assertEquals(1, respQueue.size());

        ChunkPongResponse resp = respQueue.take();
        PongResponse header = PongResponse.parseFrom(resp.getHeader());
        PongResponse payload = PongResponse.parseFrom(resp.getPayload());
        assertEquals(header, payload);

        assertEquals(sequence + numPings - 1, header.getLastSequence());
        assertEquals(numPings, header.getNumPingReceived());
        assertEquals(0, header.getSlotId());
    }

}
