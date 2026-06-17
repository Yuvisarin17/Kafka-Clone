package com.kafka.clone.broker;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

/**
 * Defines the wire protocol for Kafka clone.
 */
public class Protocol {
    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // Broker response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte ERROR_RESPONSE = 0x1F;

    // Internal broker communication
    public static final byte REPLICATE = 0x21;
    public static final byte REPLICATE_ACK = 0x22;
    public static final byte TOPIC_NOTIFICATION = 0x23;

    /**
     * Send an error response to the client
     */
    public static void sendErrorResponse(SocketChannel channel, String errorMessage) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(3 + errorMessage.length);
        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short) errorMessage.length());
        buffer.put(errorMessage.getBytes());
        buffer.flip();
        channel.write(buffer);
    }

    /**
     * Encode a producer request
     */
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + topic.length() + 4 + 4 + message.length);
        buffer.put(PRODUCE);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    /**
     *  Encode a fetch request
     */
    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != FETCH_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new FetchResult(new byte[0][], error);
            }
            return new FetchResult(new byte[0][], "Invalid response type");
        }
        
        int messageCount = buffer.getInt();
        byte[][] messages = new byte[messageCount][];
        
        for (int i = 0; i < messageCount; i++) {
            long offset = buffer.getLong(); // Skip offset
            int messageSize = buffer.getInt();
            messages[i] = new byte[messageSize];
            buffer.get(messages[i]);
        }
        
        return new FetchResult(messages, null);
    }
}

