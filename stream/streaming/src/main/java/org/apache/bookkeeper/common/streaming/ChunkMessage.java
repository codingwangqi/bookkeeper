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

import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnsafeByteOperations;
import com.google.protobuf.WireFormat;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.internal.ReadableBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import java.io.IOException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.streaming.grpc.DrainableByteBufInputStream;
import org.apache.bookkeeper.common.streaming.grpc.GetReadableBuffer;
import org.apache.bookkeeper.common.streaming.proto.DataChunk;

/**
 * An in-memory representation of {@link DataChunk} in the stream.
 */
@Slf4j
public class ChunkMessage implements AutoCloseable {

    private static final int HEADER_TAG =
        (DataChunk.HEADER_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
    private static final int PAYLOAD_TAG =
        (DataChunk.PAYLOAD_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;

    private static ChunkMessage frame(ByteBufAllocator allocator, InputStream in) {
        try {
            return doFrame(allocator, in);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private static ChunkMessage doFrame(ByteBufAllocator allocator, InputStream in) throws IOException {
        ByteBuf header = null;
        ByteBuf payload = null;
        while (in.available() > 0) {
            int tag = readRawVarint32(in);
            switch (tag) {
                case HEADER_TAG:
                    header = doFrameBuffer(allocator, in);
                    break;
                case PAYLOAD_TAG:
                    payload = doFrameBuffer(allocator, in);
                    break;
                default:
                    // skip unknown fields
                    int size = readRawVarint32(in);
                    log.warn("Ignored unknown tag : {}, size = {}", tag, size);
                    in.skip(size);
                    break;
            }
        }
        return new ChunkMessage(header, payload);
    }

    private static ByteBuf doFrameBuffer(ByteBufAllocator allocator, InputStream in) throws IOException {
        int size = readRawVarint32(in);
        ReadableBuffer readableBuffer = GetReadableBuffer.getReadableBuffer(in);
        ByteBuf buffer;
        if (null == readableBuffer) {
            // this is the slow path
            buffer = allocator.heapBuffer(size);
            byte[] underlyingArray = buffer.array();
            int arrayOffset = buffer.arrayOffset() + buffer.readerIndex();
            ByteStreams.readFully(in ,underlyingArray, arrayOffset, size);
        } else {
            // this is the fast path
            buffer = allocator.buffer(size);
            readableBuffer.readBytes(buffer.nioBuffer(0, size));
        }
        buffer.writerIndex(size);
        return buffer;
    }

    private static int readRawVarint32(InputStream is) throws IOException {
        int firstByte = is.read();
        return CodedInputStream.readRawVarint32(firstByte, is);
    }

    public static Marshaller<ChunkMessage> createMarshaller(ByteBufAllocator allocator) {
        return new ChunkMessageMarshaller(allocator);
    }

    private static class ChunkMessageMarshaller implements MethodDescriptor.Marshaller<ChunkMessage> {

        private final ByteBufAllocator allocator;

        ChunkMessageMarshaller(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public InputStream stream(ChunkMessage msg) {
            try {
                return msg.asInputStream(allocator);
            } finally {
                msg.close();
            }
        }

        @Override
        public ChunkMessage parse(InputStream stream) {
            return ChunkMessage.frame(allocator, stream);
        }
    }

    private final ByteBuf header;
    private final ByteBuf payload;

    public ChunkMessage(ByteBuf header, ByteBuf payload) {
        this.header = header;
        this.payload = payload;
    }

    public ByteBuf getHeader() {
        return header;
    }

    public ByteBuf getPayload() {
        return payload;
    }

    public InputStream asInputStream(ByteBufAllocator allocator) {
        try {
            return doAsInputStream(allocator);
        } catch (Exception e) {
            throw new RuntimeException("Unexpected IO exception", e);
        }
    }

    private InputStream doAsInputStream(ByteBufAllocator allocator) throws Exception {
        int estimatedSize =
              Integer.BYTES             /* header tag */
            + Integer.BYTES             /* header size */
            + header.readableBytes()    /* header bytes */
            + Integer.BYTES             /* payload tag */
            + Integer.BYTES;            /* payload size */

        ByteBuf initialBuf = allocator.heapBuffer(estimatedSize);

        CodedOutputStream cos = CodedOutputStream.newInstance(
            initialBuf.array(),
            initialBuf.arrayOffset(),
            estimatedSize
        );
        ByteString headerBs = UnsafeByteOperations.unsafeWrap(header.nioBuffer());
        cos.writeBytes(DataChunk.HEADER_FIELD_NUMBER, headerBs);
        cos.writeTag(DataChunk.PAYLOAD_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
        cos.writeUInt32NoTag(payload.readableBytes());
        cos.flush();
        int actualSize = cos.getTotalBytesWritten();
        initialBuf.writerIndex(actualSize);

        CompositeByteBuf bb = new CompositeByteBuf(
            allocator,
            true,
            2,
            initialBuf,
            payload.retain());
        return new DrainableByteBufInputStream(bb);
    }

    @Override
    public void close() {
        header.release();
        payload.release();
    }

}
