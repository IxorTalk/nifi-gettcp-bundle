package org.apache.nifi.processors.gettcp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import org.apache.nifi.logging.ComponentLog;

import java.util.concurrent.TimeUnit;

public class ConnectionListener implements ChannelFutureListener {

    private final Bootstrap b;
    private final ComponentLog log;
    private int reconnectDelayInSeconds;

    public ConnectionListener(Bootstrap b,ComponentLog log, int reconnectDelayInSeconds ) {
        this.b = b;
        this.log=log;
        this.reconnectDelayInSeconds = reconnectDelayInSeconds;
    }

    @Override
    public void operationComplete(ChannelFuture channelFuture) throws Exception {

        final ChannelFutureListener listener = this;
        if (!channelFuture.isSuccess()) {
            log.warn("Channel failure detected by connectionListener. Planning reconnect in " + reconnectDelayInSeconds + " seconds.");
            final EventLoop loop = channelFuture.channel().eventLoop();
            loop.schedule(new Runnable() {
                @Override
                public void run() {
                    log.warn("Reconnecting the TCP connection.");
                    b.connect().addListener(listener);
                }
            }, reconnectDelayInSeconds, TimeUnit.SECONDS);
        }
    }
}