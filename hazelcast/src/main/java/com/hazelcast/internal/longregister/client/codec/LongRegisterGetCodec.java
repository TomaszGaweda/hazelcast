/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.longregister.client.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;
import com.hazelcast.client.impl.protocol.codec.custom.*;

import javax.annotation.Nullable;

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

/*
 * This file is auto-generated by the Hazelcast Client Protocol Code Generator.
 * To change this file, edit the templates or the protocol
 * definitions on the https://github.com/hazelcast/hazelcast-client-protocol
 * and regenerate it.
 */

/**
 * Gets the current value.
 */
@Generated("d53796c3d0c3f13fc3da807d388ba2a8")
public final class LongRegisterGetCodec {
    //hex: 0xFF0300
    public static final int REQUEST_MESSAGE_TYPE = 16712448;
    //hex: 0xFF0301
    public static final int RESPONSE_MESSAGE_TYPE = 16712449;
    private static final int REQUEST_INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_RESPONSE_FIELD_OFFSET = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_RESPONSE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private LongRegisterGetCodec() {
    }

    public static ClientMessage encodeRequest(String name) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("LongRegister.Get");
        Frame initialFrame = new Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        return clientMessage;
    }

    /**
     * The name of this IAtomicLong instance.
     */
    public static String decodeRequest(ClientMessage clientMessage) {
        ForwardFrameIterator iterator = clientMessage.frameIterator();
        //empty initial frame
        iterator.next();
        return StringCodec.decode(iterator);
    }

    public static ClientMessage encodeResponse(long response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        Frame initialFrame = new Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        encodeLong(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET, response);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    /**
     * the current value
     */
    public static long decodeResponse(ClientMessage clientMessage) {
        ForwardFrameIterator iterator = clientMessage.frameIterator();
        Frame initialFrame = iterator.next();
        return decodeLong(initialFrame.content, RESPONSE_RESPONSE_FIELD_OFFSET);
    }

}
