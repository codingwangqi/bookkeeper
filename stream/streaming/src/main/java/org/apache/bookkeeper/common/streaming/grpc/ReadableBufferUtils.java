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

package org.apache.bookkeeper.common.streaming.grpc;

import io.grpc.internal.CompositeReadableBuffer;
import io.grpc.internal.ReadableBuffer;
import io.grpc.netty.NettyReadableBufferUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.InputStream;
import java.util.Queue;

/**
 * Utils for accessing readable buffers.
 */
public final class ReadableBufferUtils {

    private ReadableBufferUtils() {}

    public static ByteBuf read(InputStream is, int size) {
        ReadableBuffer readableBuffer = GetReadableBuffer.getReadableBuffer(is);
        if (null == readableBuffer) { // it is not a readable buffer :(
            return null;
        }
        return read(readableBuffer, size);
    }

    private static ByteBuf read(ReadableBuffer readableBuffer, int size) {
        if (readableBuffer instanceof CompositeReadableBuffer) {
            Queue<ReadableBuffer> internalReadableBuffers =
                GetCompositeReadableBuffer.getBuffers((CompositeReadableBuffer) readableBuffer);
            CompositeByteBuf bytebuf =
                PooledByteBufAllocator.DEFAULT.compositeBuffer(internalReadableBuffers.size());
            for (ReadableBuffer rb : internalReadableBuffers) {
                ByteBuf bb = read(rb, size);
                if (null == bb) {
                    // we find something we don't know, release the retained buffers and return null
                    bytebuf.release();
                    return null;
                } else {
                    bytebuf.addComponent(bb);
                    size -= bb.readableBytes();
                    if (size <= 0) {
                        break;
                    }
                }
            }
            return bytebuf;
        } else if (NettyReadableBufferUtils.isNettyReadableBuffer(readableBuffer)) {
            return NettyReadableBufferUtils.retainedSlice(readableBuffer);
        } else {
            // unknown type
            return null;
        }

    }

}
