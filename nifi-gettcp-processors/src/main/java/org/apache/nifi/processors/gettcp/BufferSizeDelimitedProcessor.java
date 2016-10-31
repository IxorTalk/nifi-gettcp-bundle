package org.apache.nifi.processors.gettcp;

import org.apache.nifi.logging.ComponentLog;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

public class BufferSizeDelimitedProcessor implements BufferProcessor {

    public StringBuffer processBuffer(byte delim, int nBytes, ByteBuffer buf, StringBuffer message, ComponentLog log, BlockingQueue<String> socketMessagesReceived) {
        for (int i=0 ; i<nBytes ; i++) {
            byte b  = buf.get();
            //System.out.println("b = " + String.format("[%02x] ", b));
            if (b==delim) {
                socketMessagesReceived.offer(message.toString());
                message = new StringBuffer();
            } else {
                try {
                    message.append(new String(new byte[]{b}, "UTF-8"));
                } catch (Exception e) {
                    log.error("Error occured while converting byte to string.",e);
                }
            }

        }
        return message;
    }
}