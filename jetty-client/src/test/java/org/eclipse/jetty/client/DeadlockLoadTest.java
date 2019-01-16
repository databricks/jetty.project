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
import static sun.misc.PostVMInitHook.run;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.io.Buffer;
import org.eclipse.jetty.io.ByteArrayBuffer;
import org.junit.Test;

public class DeadlockLoadTest extends AbstractConnectionTest
{
    protected HttpClient newHttpClient()
    {
        HttpClient httpClient = new HttpClient();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SOCKET);
        return httpClient;
    }

    private class MyServer {
        ServerSocket _socket;
        Thread _thread;
        final AtomicBoolean _started = new AtomicBoolean();
        final AtomicBoolean _stopped = new AtomicBoolean();


        public int start() throws IOException {
            if(!_started.getAndSet(true)){
                _socket = new ServerSocket();
                _socket.setSoTimeout(1000);
                _socket.bind(null);
                _thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        MyServer.this._run();
                    }
                });
                _thread.start();
                return _socket.getLocalPort();
            } else {
                throw new RuntimeException("already started.");
            }
        }

        private void _run() {
            try {
                final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                        64,
                        64,
                        10,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<Runnable>());
                // Selector: multiplexor of SelectableChannel objects
                final Selector selector = Selector.open(); // selector is open here

                // ServerSocketChannel: selectable channel for stream-oriented listening sockets
                final ServerSocketChannel sock = ServerSocketChannel.open();
                // Binds the channel's socket to a local address and configures the socket to listen for connections
                sock.bind(null);

                // Adjusts this channel's blocking mode.
                sock.configureBlocking(true);
                final SelectionKey k = sock.register(selector, sock.validOps());
                while(!_stopped.get()) {
                    try {
                        final Socket remote = _socket.accept();
                        try {




                            threadPool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        selector.select(1000);
                                        BufferedReader input = new BufferedReader(new InputStreamReader(remote.getInputStream()));
                                        OutputStream output = remote.getOutputStream();
                                        remote.setSoTimeout(1000);
                                        Random rnd = new Random(System.currentTimeMillis());
                                        while (true || rnd.nextDouble() < .9) {
                                            String line;
                                            int contentLength = -1;
                                            StringBuffer sb = new StringBuffer();
                                            int lines = 0;
                                            while ((line = input.readLine()) != null && line.length() != 0) {
                                                lines++;
                                                sb.append(line);
                                                if (line.startsWith("Content-Length:")) {
                                                    String s = line.substring("Content-Length:".length()).trim();
                                                    contentLength = Integer.valueOf(s);
                                                }
                                            }
                                            if (lines == 0) break; // no valid message, we're done
                                            if (contentLength == -1) {
                                                throw new RuntimeException("Not a valid message: '" + sb.toString() + "'");
                                            }
                                            sb = new StringBuffer();
                                            for (int x = 0; x < contentLength; ++x) {
                                                sb.append((char) input.read());
                                            }
//                                            System.out.println("Received Content: '" + sb.toString() + "'");

                                            output.write("HTTP/1.1 200 OK\r\n".getBytes("UTF-8"));
                                            String contentLengthStr = "Content-Length: " + contentLength + "\r\n";
                                            output.write(contentLengthStr.getBytes("UTF-8"));
                                            output.write("\r\n".getBytes("UTF-8"));
                                            output.write(sb.toString().getBytes());
                                            output.flush();
                                        }
                                        output.close();
                                        input.close();
                                    }catch(SocketTimeoutException ex) {
                                        System.out.println("closing socket " + remote + " after timeout!");
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    } finally {
                                        try {
                                            remote.close();
                                        } catch (IOException e) {}
                                    }
                                }
                            });
                        } catch(RejectedExecutionException rex){
                            System.exit(-1);
                            remote.close();
                        }
                    } catch (SocketTimeoutException ex){}
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        public int port(){return _socket.getLocalPort();}
        public void stop() throws InterruptedException, IOException {
            if(!_stopped.getAndSet(true)){
                _thread.join(1000);
                _socket.close();
            }
        }
    }


    private class State {
        public final int numRequests;
        final AtomicInteger requests = new AtomicInteger();
        final ConcurrentHashMap<String, AtomicInteger> errorCounts = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, Boolean> messages = new ConcurrentHashMap<>();
        final AtomicInteger succs = new AtomicInteger();
        final AtomicInteger fails = new AtomicInteger();
        final Object _cond = new Object();
        private boolean isDone;
        public State(int numRequests) {this.numRequests = numRequests;}
        private AtomicInteger completedRequests = new AtomicInteger();
        public void requestSent(){
            requests.incrementAndGet();
        }
        public void requestCompleted(String err){
            if(err == null) {
                succs.incrementAndGet();
            } else {
                fails.incrementAndGet();
                errorCounts.putIfAbsent(err, new AtomicInteger());
                errorCounts.get(err).incrementAndGet();
            }
            if(completedRequests.incrementAndGet() == numRequests) {
                synchronized (_cond) {
                    if(!isDone) {
                        isDone = true;
                        _cond.notifyAll();
                    }
                }
            }
        }
        private void waitForCompletion(){
            synchronized (_cond) {
                while(!isDone) {
                    try {
                        System.out.println(this.toString());
                        System.out.flush();
                        _cond.wait(5*1000);
                    } catch (InterruptedException e) {}
                }
            }
        }

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

        public void messageSent(String msg) {
            messages.putIfAbsent(msg, true);
        }

        public void messageReceived(String msg){
            assert messages.remove(msg);
        }
    }

    @Test
    public void testDeadlocks() throws Exception {
        System.out.println("Started");
        // Differently from the SelectConnector, the SocketConnector cannot detect server closes.
        // Therefore, upon a second send, the exchange will fail.
        // Applications needs to retry it explicitly.
        MyServer server = new MyServer();

        final int serverPort = server.start();

        final HttpClient httpClient = this.newHttpClient();
//        httpClient.setConnectBlocking(false);
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        httpClient.setConnectTimeout(60000);
        httpClient.setIdleTimeout(120000);
        httpClient.setMaxConnectionsPerAddress(64);
        httpClient.start();

        ThreadPoolExecutor pool = new ThreadPoolExecutor(16, 16, 10,
                TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100000));


        try
        {
            final State st = new State(1024);

            for(int i = 0; i < st.numRequests; i ++) {
                if(i != 0 && (i % 1000) == 0) {
                    Thread.sleep(100);
//                    System.out.println(st.toString());
                }
                final String message = "Message("  + i + ").";

                pool.execute(new Runnable() {
                    @Override
                    public void run() {
                        HttpExchange exchange = new HttpExchange() {
                            String _content;
                            int contentLength = 0;
                            @Override protected void onResponseHeader(Buffer name, Buffer value){
                                String nameStr = name.toString("utf-8");
                                String valStr = value.toString("utf-8");
                                if(nameStr.equals("Content-Length")) {
                                    contentLength = Integer.valueOf(valStr);
                                }
                            }

                            @Override protected void onResponseContent(Buffer content){
                                byte [] bits = new byte[contentLength];
                                int read = content.get(bits, 0, contentLength);
                                while(read < contentLength) {
                                    read += content.get(bits, read, contentLength - read);
                                }
                                String received = new String(bits);
                                assertEquals(_content, received);
                                st.messageReceived(new String(bits));

                            }

                            @Override
                            protected void onResponseComplete() throws IOException {
                                st.requestCompleted(null);
                            }

                            @Override
                            protected void onExpire(){st.requestCompleted("Expired!");}


                            @Override
                            public void onException(Throwable ex) {
//                                System.out.println("message sent failed for msg '" + _content + "'");
                                st.requestCompleted(ex.toString());
                                st.messageReceived(_content);
                            }

                            @Override
                            public void onConnectionFailed(Throwable ex) {
//                                System.out.println("message sent failed for msg '" + _content + "'");
                                st.requestCompleted(ex.toString());
                                st.messageReceived(_content);
                            }

                            @Override public void setRequestContent(Buffer requestContent) {
                                _content = requestContent.toString("utf-8");
                                super.setRequestContent(requestContent);
                            }
                        };
                        exchange.setRequestContent(new ByteArrayBuffer(message));
                        exchange.setAddress(new Address("localhost", serverPort));
                        exchange.setMethod("POST");
                        exchange.setRequestURI("/some/path");
                        try {
                            httpClient.send(exchange);
                        } catch (IOException e) {}
                    }
                });
                st.requestSent();
                st.messageSent(message);
            }
            st.waitForCompletion();
            System.out.println(st.toString());
        } finally {
            server.stop();
            httpClient.stop();
        }
    }

    public void testIdleConnection() throws Exception
    {
        super.testIdleConnection();
    }
}
