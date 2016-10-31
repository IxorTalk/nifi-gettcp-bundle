package org.apache.nifi.processors.gettcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.*;

/**
 * Created by ddewaele on 28/10/16.
 */
public class NioClient {

    private static final int EOT = 4;

    private final BlockingQueue<String> socketMessagesReceived = new ArrayBlockingQueue<>(256);

    private static final String SERVER_ADDRESS="127.0.0.1";
    private static final int PORT = 4001;

    private SocketChannel client = null;
    private SocketRecveiverThread socketRecveiverThread = null;
    private Future receiverThreadFuture = null;
    private ExecutorService executorService = Executors.newFixedThreadPool(1);

    public static void main(String[] args) throws Exception {
        NioClient client = new NioClient();
        client.connect();

    }

    public void connect() throws Exception {

        client = SocketChannel.open();
        InetSocketAddress inetSocketAddress = new InetSocketAddress(SERVER_ADDRESS, PORT);
        client.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        client.setOption(StandardSocketOptions.SO_RCVBUF, 256);
        client.connect(inetSocketAddress);
        client.configureBlocking(true);
        socketRecveiverThread = new SocketRecveiverThread(client, 2048);
        if (executorService.isShutdown()) {
            executorService = Executors.newFixedThreadPool(1);
        }
        receiverThreadFuture = executorService.submit(socketRecveiverThread);

        while (true) {
            if (client.isOpen()) {
                final String messages = socketMessagesReceived.poll(100, TimeUnit.MILLISECONDS);

                if (messages == null) {
                    System.out.println("Read : EMPTY");
                    //return;
                } else {
                    System.out.println("Read : " + messages); // + " - " + byteArrayToHex(messages.getBytes()));
                }

            }
        }
    }


    private void disconnect() throws Exception {

            //log.info("Stop processing....");
            socketRecveiverThread.stopProcessing();
            receiverThreadFuture.cancel(true);
            executorService.shutdown();
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();

                if (!executorService.awaitTermination(5, TimeUnit.SECONDS))
                    System.out.println("Executor service for receiver thread did not terminate");
            }
            client.close();

    }


    public static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for(byte b: a)
            sb.append(String.format(" [%02x]", b & 0xff));
        return sb.toString();
    }

    private class SocketRecveiverThread implements Runnable {

        private SocketChannel socketChannel = null;
        private boolean keepProcessing = true;
        private int bufferSize;

        SocketRecveiverThread(SocketChannel client, int bufferSize) {
            socketChannel = client;
            this.bufferSize = bufferSize;
        }

        void stopProcessing(){
            keepProcessing = false;
        }
        public void run() {
            System.out.println("Starting to receive messages with buf " + bufferSize);

            int nBytes = 0;
            ByteBuffer buf = ByteBuffer.allocate(bufferSize);
            StringBuffer message = new StringBuffer();

            byte delim = (byte)Integer.parseInt("04", 16);

            try {
                while (keepProcessing) {
                    if(socketChannel.isOpen() && socketChannel.isConnected()) {

                        while ((nBytes = socketChannel.read(buf)) > 0) {

                            buf.flip();

                            //System.out.println("Read " + nBytes + " from socket = " + byteArrayToHex(buf.array()).replaceAll("\\[00]\\]",""));

                            //if (indexOf(buf.array(),new byte[]{0x04})!=-1) {

                                for (int i=0 ; i<nBytes ; i++) {
                                    byte b  = buf.get();
                                    //System.out.println("b = " + String.format("[%02x] ", b));
                                    if (b==delim) {
                                        //if (buf.hasRemaining()) buf.get();
                                        socketMessagesReceived.offer(message.toString());
                                        message = new StringBuffer();
                                    } else {
                                        message.append(new String(new byte[]{b}, "UTF-8"));
                                    }

                                }

                                //System.out.println("____________Received Message: " + message);


//                            } else {
//
//                                    Charset charset = Charset.forName(StandardCharsets.UTF_8.name());
//                                    CharsetDecoder decoder = charset.newDecoder();
//                                    CharBuffer charBuffer = decoder.decode(buf);
//                                    message.append(charBuffer.toString());
//                                    //System.out.println("____________Received Message_: " + message + " - " + byteArrayToHex(message.toString().getBytes()));
//                            }


//                            message = new StringBuffer();
//                            Charset charset = Charset.forName(StandardCharsets.UTF_8.name());
//                            CharsetDecoder decoder = charset.newDecoder();
//                            CharBuffer charBuffer = decoder.decode(buf);
//                            message.append(charBuffer.toString());
//                            System.out.println("____________Received Message_: " + message);
//                            socketMessagesReceived.offer(message.toString());


                            buf.clear();

                        }
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();

            }


        }
//
//        private int containsDelimiter(ByteBuffer buf,int readCount, int delimiter) {
//            byte [] bytes = buf.array();
//            int i=0;
//            for (int i=0 ; i<readCount ; i++) {
//                bytes.
//                i++;
//                byte b  = buf.get();
//                System.out.println("b = " + String.format("[%02x] ", b));
//                if (b==delimiter) {
//                    if (buf.hasRemaining()) buf.get();
//                    break;
//                }
//            }
//        }
//
//
//        private int indexOf(byte[] outerArray, byte[] smallerArray) {
//            for(int i = 0; i < outerArray.length - smallerArray.length+1; ++i) {
//                boolean found = true;
//                for(int j = 0; j < smallerArray.length; ++j) {
//                    if (outerArray[i+j] != smallerArray[j]) {
//                        found = false;
//                        break;
//                    }
//                }
//                if (found) return i;
//            }
//            return -1;
//        }

    }
}