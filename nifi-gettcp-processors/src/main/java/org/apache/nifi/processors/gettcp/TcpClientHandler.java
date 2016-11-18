/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.gettcp;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.nifi.logging.ComponentLog;

import java.net.ConnectException;
import java.nio.charset.Charset;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


@Sharable
public class TcpClientHandler extends SimpleChannelInboundHandler<Object> {

    private final GetTCP client;
    private long startTime = -1;
    private static final int READ_TIMEOUT = 50;
    private static final int RECONNECT_DELAY = 5;
    private final BlockingQueue<String> socketMessagesReceived = new ArrayBlockingQueue<>(256);
    private ComponentLog log = null;
    private BufferProcessor bufferProcessor;
    StringBuffer message = new StringBuffer();

    public TcpClientHandler(GetTCP client) {
        this.client = client;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (startTime < 0) {
            startTime = System.currentTimeMillis();
        }
        log.info("Connected to: " + ctx.channel().remoteAddress());
    }

    public void setLog(ComponentLog log) {
        this.log = log;
    }

    public void setBufferProcessor(BufferProcessor bufferProcessor) {
        this.bufferProcessor = bufferProcessor;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (!(evt instanceof IdleStateEvent)) {
            return;
        }

        IdleStateEvent e = (IdleStateEvent) evt;
        if (e.state() == IdleState.READER_IDLE) {
            // The connection was OK but there was no traffic for last period.
            log.info("Disconnecting due to no inbound traffic");
            ctx.close();
        }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        log.info("Disconnected from: " + ctx.channel().remoteAddress());
        log.info("Sleeping for: " + RECONNECT_DELAY + 's');

        final EventLoop loop = ctx.channel().eventLoop();
        loop.schedule(new TimerTask() {
            @Override
            public void run() {
                log.info("Reconnecting to: " + ctx.channel().remoteAddress());
                client.configureBootstrap(new Bootstrap(), loop).connect();
            }
        }, RECONNECT_DELAY, TimeUnit.SECONDS);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof ConnectException) {
            startTime = -1;
        }
        log.error("Failed to connect: " + cause.getMessage());

        cause.printStackTrace();
        ctx.close();
    }


    public String poll () throws InterruptedException {
        return socketMessagesReceived.poll(100, TimeUnit.MILLISECONDS);
    }

    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
        ByteBuf in = (ByteBuf) o;
        byte delim = (byte)Integer.parseInt("04", 16);

        try {
            while (in.isReadable()) {
                byte b = in.readByte();
                if (b == delim) {
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
        } catch (Exception e) {
            log.error("error reading messages: " + e.toString());
            e.printStackTrace();
            throw e;
        } finally {
        }
    }
}