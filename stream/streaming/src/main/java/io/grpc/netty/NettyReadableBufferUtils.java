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

package io.grpc.netty;

import io.grpc.internal.ReadableBuffer;
import io.netty.buffer.ByteBuf;

/**
 * BK Utils to access grpc details.
 */
public final class NettyReadableBufferUtils {

    private NettyReadableBufferUtils() {}

    /**
     * Returns a retained slice of this readable buffer's sub-region.
     *
     * @param readableBuffer readable buffer to retain the slice
     * @return the retained slice of the readable buffer.
     */
    public static ByteBuf retainedSlice(ReadableBuffer readableBuffer) {
        if (isNettyReadableBuffer(readableBuffer)) {
            return ((NettyReadableBuffer) readableBuffer).buffer().retainedSlice();
        } else {
            return null;
        }
    }

    /**
     * Check if a <code>buffer</code> is a netty based readable buffer.
     *
     * @param buffer buffer to check
     * @return true if buffer is the netty readable buffer; otherwise false
     */
    public static boolean isNettyReadableBuffer(ReadableBuffer buffer) {
        return buffer instanceof NettyReadableBuffer;
    }

}
