//
//  ========================================================================
//  Copyright (c) 1995-2016 Mort Bay Consulting Pty. Ltd.
//  ------------------------------------------------------------------------
//  All rights reserved. This program and the accompanying materials
//  are made available under the terms of the Eclipse Public License v1.0
//  and Apache License v2.0 which accompanies this distribution.
//
//      The Eclipse Public License is available at
//      http://www.eclipse.org/legal/epl-v10.html
//
//      The Apache License v2.0 is available at
//      http://www.opensource.org/licenses/apache2.0.php
//
//  You may elect to redistribute this code under either of these licenses.
//  ========================================================================
//

package org.eclipse.jetty.client;

import static org.junit.Assert.assertEquals;

import java.io.*;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.io.Buffer;
import org.eclipse.jetty.io.ByteArrayBuffer;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.Test;

public class DeadlockLoadTest extends AbstractConnectionTest
{
    protected HttpClient newHttpClient()
    {
        HttpClient httpClient = new HttpClient();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SOCKET);
        return httpClient;
    }

    private class EchoRESTServer {
        Thread _thread;
        final AtomicBoolean _started = new AtomicBoolean();
        final AtomicBoolean _stopped = new AtomicBoolean();
        Selector selector;
        ServerSocketChannel sock;
        SelectionKey _key;
        double _errorRate;
        int _connectionsCreated;

        public EchoRESTServer(double errorRate) {
            _errorRate = errorRate;
        }

        public int start() throws IOException {
            if(!_started.getAndSet(true)){
                selector = Selector.open();
                sock = ServerSocketChannel.open();
                sock.bind(null);
                sock.configureBlocking(false);
                // Adjusts this channel's blocking mode.
                String addr = sock.getLocalAddress().toString();
                int port = Integer.valueOf(addr.substring(addr.lastIndexOf(":") + 1));
                _key = sock.register(selector, sock.validOps());
                _thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        EchoRESTServer.this._run();
                    }
                });
                _thread.start();
                return port;
            } else {
                throw new RuntimeException("already started.");
            }
        }

        private String readInput( SocketChannel chan, ByteBuffer inputBuffer, int maxAttempts) throws IOException {
            for(int i  = 0; i < maxAttempts; ++i) {
                if (chan.read(inputBuffer) > 0)
                    return new String(Arrays.copyOf(inputBuffer.array(), inputBuffer.position()));
            }
            throw new RuntimeException("no input");
        }

        private void _run() {
            ByteBuffer outputBuffer = ByteBuffer.allocate(1024);
            Random rnd = new Random();
            while (!_stopped.get()) {
                try {
                    int keycnt = selector.select(1000);
                    if(keycnt == 0) continue;
                    Set<SelectionKey> keys = selector.selectedKeys();
                    for (Iterator<SelectionKey> it = keys.iterator(); it.hasNext(); ) {
                        final SelectionKey k = it.next();
                        if (k.isValid()) {
                            if (k.isAcceptable()) {
                                SocketChannel remote = sock.accept();
                                remote.configureBlocking(false);
                                remote.register(selector, SelectionKey.OP_READ);//
                                _connectionsCreated++;
                            } else if (k.isReadable()) {
                                final SocketChannel chan = (SocketChannel) k.channel();
                                if (k.attachment() == null) {
                                    k.attach(ByteBuffer.allocate(1024));
                                }
                                final ByteBuffer inputBuffer = (ByteBuffer) k.attachment();
                                try {
                                    if(rnd.nextDouble() < _errorRate) {
                                        chan.close();
                                        k.cancel();
                                        continue;
                                    }
                                    String result = "";

                                    int messageBodyStart;
                                    while ((messageBodyStart = result.indexOf("\r\n\r\n")) == -1)
                                        result = readInput(chan, inputBuffer, 1000);
                                    messageBodyStart += 4;
                                    String kwrd = "Content-Length:";
                                    int contentLengthStart = result.indexOf(kwrd) + kwrd.length();
                                    int contentLengthEnd = result.indexOf("\r\n", contentLengthStart);
                                    int contentLength = Integer.valueOf(
                                            result.substring(contentLengthStart, contentLengthEnd).trim());

                                    while( (messageBodyStart + contentLength) > result.length())
                                        result = readInput(chan, inputBuffer, 1000);
                                    String message = result.substring(messageBodyStart,
                                            messageBodyStart + contentLength);
                                    inputBuffer.flip();
                                    inputBuffer.position(messageBodyStart + contentLength);
                                    inputBuffer.compact();
                                    StringBuffer response = new StringBuffer();
                                    response.append("HTTP/1.1 200 OK\r\n");
                                    response.append("Content-Length: " + contentLength + "\r\n\r\n");
                                    response.append(message);
                                    byte[] bytes = response.toString().getBytes("utf-8");
                                    outputBuffer.put(bytes);
                                    outputBuffer.flip();
                                    chan.write(outputBuffer);
                                    outputBuffer.clear();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                        it.remove();
                    }
                } catch (Exception e) {}
            }
        }

        public void stop() throws InterruptedException, IOException {
            if(!_stopped.getAndSet(true)){
                _thread.join(1000);
            }
        }
    }

    private static class State {
        final int numRequests;

        public State(int numRequests) {
            this.numRequests = numRequests;
        }

        final ConcurrentHashMap<String, AtomicInteger> errorCounts = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, Boolean> messages = new ConcurrentHashMap<>();
        final AtomicInteger succs = new AtomicInteger();
        final AtomicInteger fails = new AtomicInteger();
        final AtomicInteger requests = new AtomicInteger();

        final Object _cond = new Object();
        boolean isDone = false;

        public String toString(){
            StringBuilder sb = new StringBuilder();
            sb.append("Requests total = " + numRequests);
            sb.append("\nRequests sent  = " + requests.get());
            sb.append("\nRequests succs = " + succs.get());
            sb.append("\nRequests fails = " + fails.get());
            sb.append("\nError counts   = " + errorCounts.toString());
            sb.append("\nUnaccounted message count = " + messages.size());
            if(messages.size() < 100){
                ArrayList<String> unaccountedMessages = new ArrayList<>();
                for(String s: messages.keySet()) {
                    unaccountedMessages.add(s);
                }
                Collections.sort(unaccountedMessages);
                System.out.println(unaccountedMessages);
            }
            return sb.toString();
        }

        private void requestCompleted(){
            if((succs.get() + fails.get()) == numRequests){
                synchronized (_cond) {
                    isDone = true;
                    _cond.notifyAll();
                }
            }
        }

        public void messageSent(String msg) {
            requests.incrementAndGet();
            messages.putIfAbsent(msg, true);
        }

        public void messageReceived(String msg){
            succs.incrementAndGet();
            assert messages.remove(msg) != null;
            requestCompleted();
        }

        public void messageFailed(String msg, String err) {
            assert messages.remove(msg) != null;
            fails.incrementAndGet();
            errorCounts.putIfAbsent(err, new AtomicInteger(0));
            errorCounts.get(err).incrementAndGet();
            requestCompleted();
        }

        void waitForCompletion(){
            synchronized (_cond) {
                while(!isDone) {
                    try {
                        _cond.wait(10*1000);
                    } catch (InterruptedException e) {}
                }
            }
        }
    }

    private void send(HttpClient httpClient, int serverPort, final int numRequests) throws Exception {
        final State st = new State(numRequests);
        final Random rnd = new Random();
        for (int i = 0; i < numRequests; i++) {
            final StringBuffer messageBuf = new StringBuffer("Message(" + i + "): ");
            int d;
            while ((d = rnd.nextInt(100)) < 90) {
                messageBuf.append(d);
            }
            final String message = messageBuf.toString();
            HttpExchange exchange = new HttpExchange() {
                String _content;
                int contentLength = 0;

                @Override
                protected void onResponseHeader(Buffer name, Buffer value) {
                    String nameStr = name.toString("utf-8");
                    String valStr = value.toString("utf-8");
                    if (nameStr.equals("Content-Length")) {
                        contentLength = Integer.valueOf(valStr);
                    }
                }

                @Override
                protected void onResponseContent(Buffer content) {
                    byte[] bits = new byte[contentLength];
                    int read = content.get(bits, 0, contentLength);
                    while (read < contentLength) {
                        read += content.get(bits, read, contentLength - read);
                    }
                    String received = new String(bits);
                    assertEquals(_content, received);
                    st.messageReceived(new String(bits));
                    st.requestCompleted();
                }

                @Override
                protected void onResponseComplete() throws IOException {
                }

                @Override
                protected void onExpire() {
                    st.messageFailed(_content, "Expired!");
                }

                @Override
                public void onException(Throwable ex) {
                    st.messageFailed(_content, ex.toString());
                }

                @Override
                public void onConnectionFailed(Throwable ex) {
                    st.messageFailed(_content, ex.toString());
                }

                @Override
                public void setRequestContent(Buffer requestContent) {
                    _content = requestContent.toString("utf-8");
                    super.setRequestContent(requestContent);
                    assert _content != null;
                }
            };
            byte[] messageBits = message.getBytes("utf-8");
            if (messageBits.length > 512)
                messageBits = Arrays.copyOf(messageBits, 512);
            exchange.setRequestContent(new ByteArrayBuffer(messageBits));
            exchange.setAddress(new Address("localhost", serverPort));
            exchange.setMethod("POST");
            exchange.setRequestURI("/some/path");
            st.messageSent(message);
            httpClient.send(exchange);
        }
        st.waitForCompletion();
        System.out.println(st.toString());
    }



    @Test
    public void testDeadlocks() throws Exception {
        EchoRESTServer server = new EchoRESTServer(0.125);
        final int serverPort = server.start();
        final HttpClient httpClient = this.newHttpClient();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        httpClient.setConnectTimeout(60000);
        httpClient.setIdleTimeout(120000);
        httpClient.start();
        try {
            Thread [] threads = new Thread[10];
            for(int i = 0; i < threads.length; ++i) {
                threads[i] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            send(httpClient, serverPort, 1 << 18);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
            for(Thread t: threads) {
                t.start();
                Thread.sleep(100);
            }
            for(Thread t: threads) t.join();
        } finally {
            server.stop();
            System.out.println("NUM CONNECTIONS CREATED = " + server._connectionsCreated);
            httpClient.stop();
        }
    }
}
