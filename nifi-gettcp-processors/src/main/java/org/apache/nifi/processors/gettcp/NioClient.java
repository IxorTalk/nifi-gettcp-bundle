package org.apache.nifi.processors.gettcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

/**
 * Created by ddewaele on 28/10/16.
 */
public class NioClient {

    private final BlockingQueue<String> socketMessagesReceived = new ArrayBlockingQueue<>(256);

    private static final String SERVER_ADDRESS="192.168.1.254";
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
        client.setOption(StandardSocketOptions.SO_RCVBUF, 2048);
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
                    System.out.println("Read : " + messages + " - " + byteArrayToHex(messages.getBytes()));
                }

            }
        }
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
            try {
                while (keepProcessing) {
                    if(socketChannel.isOpen() && socketChannel.isConnected()) {
                        while ((nBytes = socketChannel.read(buf)) > 0) {
                            System.out.println("Read " + nBytes + " from socket");
                            buf.flip();
                            Charset charset = Charset.forName(StandardCharsets.UTF_8.name());
                            CharsetDecoder decoder = charset.newDecoder();
                            CharBuffer charBuffer = decoder.decode(buf);

                            final String message = charBuffer.toString();
                            System.out.println("Received Message: " + message);
                            socketMessagesReceived.offer(message);
                            //buf.flip();
                            buf.clear();
                        }
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();

            }


        }

    }
}