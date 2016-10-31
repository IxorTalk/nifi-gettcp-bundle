package org.apache.nifi.processors.gettcp;


import org.apache.nifi.logging.ComponentLog;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

public interface BufferProcessor {

    StringBuffer processBuffer(byte delim, int nBytes, ByteBuffer buf, StringBuffer message, ComponentLog log, BlockingQueue<String> socketMessagesReceived);
}
