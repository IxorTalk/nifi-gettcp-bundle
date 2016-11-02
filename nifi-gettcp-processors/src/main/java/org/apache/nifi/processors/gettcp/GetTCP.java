/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.gettcp;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;

@Tags({"get", "fetch", "poll", "tcp", "ingest", "source", "input"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Connects over TCP to the provided server. When receiving data this will writes either the" +
        " full receive buffer or messages based on demarcator to the content of a FlowFile. ")
public final class GetTCP extends AbstractProcessor {

    //private static final int EOT = 4;

    public static final PropertyDescriptor SERVER_ADDRESS = new PropertyDescriptor
            .Builder().name("Server Address")
            .description("The address of the server to connect to")
            .required(true)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .build();


    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("Port")
            .description("The port to connect to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECEIVE_BUFFER_SIZE = new PropertyDescriptor
            .Builder().name("Receive Buffer Size")
            .description("The size of the buffer to receive data in")
            .required(false)
            .defaultValue("2048")
            .addValidator(StandardValidators.createLongValidator(1, 4096, true))
            .build();

    public static final PropertyDescriptor KEEP_ALIVE = new PropertyDescriptor
            .Builder().name("Keep Alive")
            .description("This determines if TCP keep alive is used.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor MSG_DELIMITER = new PropertyDescriptor
            .Builder().name("Message Delimiter")
            .description("Bytesequence used to split flowfiles. when not provided, the flowfiles will be split according to buffer size.")
            .required(false)
            .defaultValue("04")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("The relationship that all sucessful messages from the WebSocket will be sent to")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("The relationship that all failed messages from the WebSocket will be sent to")
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    /*
    * Will ensure that the list of property descriptors is build only once.
    * Will also create a Set of relationships
    */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(SERVER_ADDRESS);
        _propertyDescriptors.add(PORT);
        _propertyDescriptors.add(RECEIVE_BUFFER_SIZE);
        _propertyDescriptors.add(KEEP_ALIVE);
        _propertyDescriptors.add(MSG_DELIMITER);


        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    private SocketChannel client = null;
    private SocketRecveiverThread socketRecveiverThread = null;
    private Future receiverThreadFuture = null;
    private ExecutorService executorService = Executors.newFixedThreadPool(1);
    private BufferProcessor bufferProcessor = null;
    private InetSocketAddress inetSocketAddress = null;

    private Socket clientSocket;

    /**
     * Bounded queue of messages events from the socket.
     */
    private final BlockingQueue<String> socketMessagesReceived = new ArrayBlockingQueue<>(256);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        connect(context);
    }

    @OnStopped
    public void tearDown() throws ProcessException {
        disconnect();
    }

    private void connect(ProcessContext context) {
        try {

            final ComponentLog log = getLogger();
            //log.info("Creating a new SocketChannel");
            client = SocketChannel.open();
            inetSocketAddress = new InetSocketAddress(context.getProperty(SERVER_ADDRESS).getValue(),
                    context.getProperty(PORT).asInteger());
            client.setOption(StandardSocketOptions.SO_KEEPALIVE,context.getProperty(KEEP_ALIVE).asBoolean());
            client.setOption(StandardSocketOptions.SO_RCVBUF,context.getProperty(RECEIVE_BUFFER_SIZE).asInteger());
            client.connect(inetSocketAddress);
            client.configureBlocking(false);

            if (!context.getProperty(MSG_DELIMITER).getValue().isEmpty()) {
                bufferProcessor = new ByteSequenceDelimitedProcessor();
            } else {
                bufferProcessor = new BufferSizeDelimitedProcessor();
            }

            socketRecveiverThread = new SocketRecveiverThread(client,context.getProperty(RECEIVE_BUFFER_SIZE).asInteger(),context.getProperty(MSG_DELIMITER).getValue(),log,bufferProcessor);
            if(executorService.isShutdown()){
                executorService = Executors.newFixedThreadPool(1);
            }
            receiverThreadFuture = executorService.submit(socketRecveiverThread);

        } catch (IOException e) {
            throw new ProcessException(e);
        }
    }

    private void disconnect() {
        try {
            //log.info("Stop processing....");
            socketRecveiverThread.stopProcessing();
            receiverThreadFuture.cancel(true);
            executorService.shutdown();
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();

                    if (!executorService.awaitTermination(5, TimeUnit.SECONDS))
                        getLogger().error("Executor service for receiver thread did not terminate");
            }
            client.close();
        } catch (final Exception e) {
            throw new ProcessException(e);
        }
    }



    public static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for(byte b: a)
            sb.append(String.format(" [%02x]", b & 0xff));
        return sb.toString();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        try {

            final ComponentLog log = getLogger();

            if(client.isOpen()) {
                final String messages = socketMessagesReceived.poll(100, TimeUnit.MILLISECONDS);

                if (messages == null) {
                    //log.error("Read : EMPTY");
                    return;
                } else {
                    log.error("Read : " + messages + " - " + byteArrayToHex(messages.getBytes()));
                }

                // final Map<String, String> attributes = new HashMap<>();
                FlowFile flowFile = session.create();

                flowFile = session.write(flowFile, out -> out.write(messages.getBytes()));

                flowFile = session.putAttribute(flowFile, "tcp.sender", context.getProperty(SERVER_ADDRESS).getValue());
                flowFile = session.putAttribute(flowFile, "tcp.port", context.getProperty(PORT).getValue());

                session.transfer(flowFile, REL_SUCCESS);

            }
        } catch (InterruptedException exception){
            throw new ProcessException(exception);
        }
    }

    private class SocketRecveiverThread implements Runnable {

        private SocketChannel socketChannel = null;
        private boolean keepProcessing = true;
        private int bufferSize;
        private ComponentLog log;
        private String delimiter;
        private BufferProcessor bufferProcessor;

        SocketRecveiverThread(SocketChannel client, int bufferSize,String delimiter, ComponentLog log, BufferProcessor bufferProcessor ) {
            socketChannel = client;
            this.bufferSize = bufferSize;
            this.delimiter = delimiter;
            this.log = log;
            this.bufferProcessor = bufferProcessor;
        }

        void stopProcessing(){
            keepProcessing = false;
        }
        public void run() {
            log.error("Starting to receive messages with buf " + bufferSize);

            byte delim = (byte)Integer.parseInt(delimiter, 16);


            int nBytes = 0;
            ByteBuffer buf = ByteBuffer.allocate(bufferSize);
            StringBuffer message = new StringBuffer();
            try {
                while (keepProcessing) {
                    if(socketChannel.isOpen() && socketChannel.isConnected()) {

                        while ((nBytes = socketChannel.read(buf)) > 0) {

                            buf.flip();

                            message = bufferProcessor.processBuffer(delim, nBytes, buf, message,log,socketMessagesReceived);

                            buf.clear();
                        }
                    }
                }

            } catch (IOException e) {
                log.error("Error occured ",e);
            }


        }
    }
}
