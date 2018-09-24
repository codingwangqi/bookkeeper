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

import io.grpc.internal.ReadableBuffer;
import java.io.InputStream;
import java.lang.reflect.Field;
import lombok.extern.slf4j.Slf4j;

/**
 * Enable access to ReadableBuffer directly to copy data from an
 * {@link io.grpc.internal.ReadableBuffers.BufferInputStream} into a target {@link java.nio.ByteBuffer} or
 * {@link io.netty.buffer.ByteBuf}.
 */
@Slf4j
public class GetReadableBuffer {

    private static final Field READABLE_BUFFER;
    private static final Class<?> BUFFER_INPUT_STREAM;

    static {
        Field tmpField = null;
        Class<?> tmpClazz = null;
        try {
            Class<?> clazz = Class.forName("io.grpc.internal.ReadableBuffers$BufferInputStream");

            Field f = clazz.getDeclaredField("buffer");
            f.setAccessible(true);
            // don't set until we've gotten past all exception cases.
            tmpField = f;
            tmpClazz = clazz;
        } catch (Exception e) {
            log.warn("Failed to access BufferInputStream", e);
        }
        READABLE_BUFFER = tmpField;
        BUFFER_INPUT_STREAM = tmpClazz;
    }

    public static ReadableBuffer getReadableBuffer(InputStream is) {

        if (BUFFER_INPUT_STREAM == null || !is.getClass().equals(BUFFER_INPUT_STREAM)) {
            return null;
        }

        try {
            return (ReadableBuffer) READABLE_BUFFER.get(is);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
