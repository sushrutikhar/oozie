/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.action.callback;


import org.apache.activemq.broker.BrokerService;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.hadoop.ActionExecutorTestCase;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;

import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.StringReader;
import java.net.ServerSocket;
import java.util.Properties;

public class TestCallbackActionExecutor extends ActionExecutorTestCase {

    private BrokerService broker;
    private Server httpServer;
    private int freeJMSPort;
    private int freeHTTPPort;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        setUpHttpServer();
        setUpJMSServer();
    }

    private void setUpJMSServer() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        freeJMSPort = findFreePort();
        broker.addConnector("tcp://localhost:" + freeJMSPort);
        broker.start();
    }

    private void setUpHttpServer() throws Exception {
        freeHTTPPort = findFreePort();
        httpServer = new Server(freeHTTPPort);
        httpServer.setStopAtShutdown(true);
        TestHandler testHandler = new TestHandler();
        httpServer.addHandler(testHandler);
        httpServer.start();
    }


    public static class TestHandler extends AbstractHandler
    {
        public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch)
                throws IOException, ServletException
        {
            Request base_request = (request instanceof Request) ? (Request)request:
                    HttpConnection.getCurrentConnection().getRequest();
            base_request.setHandled(true);
            response.setStatus(HttpServletResponse.SC_OK);
        }
    }

    public void testSetupMethods() {
        CallbackActionExecutor callbackActionExecutor = new CallbackActionExecutor();
        assertEquals("callback", callbackActionExecutor.getType());
    }

    private Context createContext(String actionXml) throws Exception {
        CallbackActionExecutor ae = new CallbackActionExecutor();

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "callback-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);
        return new Context(wf, action);
    }

    public void testSendHTTPNotification() throws Exception {
        StringBuilder elem = new StringBuilder();
        elem.append("<callback xmlns=\"uri:oozie:callback-action:0.1\">");
        elem.append("<url>http://localhost:" + freeHTTPPort + "</url>");
        elem.append("<method>HTTP_GET</method>");
        elem.append("<arg><property>");
        elem.append("<name>fookey</name><value>foovalue</value>");
        elem.append("</property></arg>");
        elem.append("<capture-output/>");
        elem.append("</callback>");
        CallbackActionExecutor callbackActionExecutor = new CallbackActionExecutor();
        Context context = createContext("callback-action");
        callbackActionExecutor.sendNotification(context, XmlUtils.parseXml(elem.toString()));
        WorkflowAction wfAction = context.getAction();
        assertEquals(WorkflowAction.Status.OK.toString(), wfAction.getExternalStatus());
        assertNotNull(wfAction.getData());
        Properties props = new Properties();
        props.load(new StringReader(wfAction.getData()));
        assertEquals("OK",props.getProperty("reason-phrase"));
        assertEquals("200",props.getProperty("status-code"));
    }

    public void testErrorOnHTTPNotification() throws Exception {
        StringBuilder elem = new StringBuilder();
        elem.append("<callback xmlns=\"uri:oozie:callback-action:0.1\">");
        elem.append("<url>http://localhost:999/app</url>");
        elem.append("<method>HTTP_GET</method>");
        elem.append("<arg><property>");
        elem.append("<name>fookey</name><value>foovalue</value>");
        elem.append("</property></arg>");
        elem.append("<capture-output/>");
        elem.append("</callback>");
        CallbackActionExecutor callbackActionExecutor = new CallbackActionExecutor();
        Context context = createContext("callback-action");
        try {
            callbackActionExecutor.sendNotification(context, XmlUtils.parseXml(elem.toString()));
            fail("Port was wrong, Should have thrown the exception");
        } catch (Exception ex) {
            assertEquals(WorkflowAction.Status.FAILED.toString(), context.getAction().getExternalStatus());
            assertEquals("CB1001", ((ActionExecutorException) ex).getErrorCode());
            assertEquals("CB1001: Error during sending HTTP request", ex.getMessage());

        }
    }

    public void testSendJMSNotification() throws Exception {
        StringBuilder elem = new StringBuilder();
        elem.append("<callback xmlns=\"uri:oozie:callback-action:0.1\">");
        elem.append("<url>tcp://localhost:" + freeJMSPort + "?queue=fooqueue</url>");
        elem.append("<method>QUEUE_OFFER</method>");
        elem.append("<arg><property>");
        elem.append("<name>fookey</name><value>foovalue</value>");
        elem.append("</property></arg>");
        elem.append("</callback>");
        CallbackActionExecutor callbackActionExecutor = new CallbackActionExecutor();
        Context context = createContext("callback-action");
        callbackActionExecutor.sendNotification(context, XmlUtils.parseXml(elem.toString()));
        WorkflowAction wfAction = context.getAction();
        assertEquals(WorkflowAction.Status.OK.toString(), wfAction.getExternalStatus());
    }

    public void testErrorOnJMSNotification() throws Exception {
        StringBuilder elem = new StringBuilder();
        elem.append("<callback xmlns=\"uri:oozie:callback-action:0.1\">");
        elem.append("<url>tcp://localhost:6161?queue=fooqueue</url>");
        elem.append("<method>QUEUE_OFFER</method>");
        elem.append("<arg><property>");
        elem.append("<name>fookey</name><value>foovalue</value>");
        elem.append("</property></arg>");
        elem.append("</callback>");
        CallbackActionExecutor callbackActionExecutor = new CallbackActionExecutor();
        Context context = createContext("callback-action");
        try {
            callbackActionExecutor.sendNotification(context, XmlUtils.parseXml(elem.toString()));
            fail("Port was wrong, Should have thrown the exception");
        } catch (Exception ex) {
            assertEquals(WorkflowAction.Status.FAILED.toString(), context.getAction().getExternalStatus());
            assertEquals("JMSException", ((ActionExecutorException) ex).getErrorCode());
        }
    }

    public void testErrorOnJMSNotification1() throws Exception {
        StringBuilder elem = new StringBuilder();
        elem.append("<callback xmlns=\"uri:oozie:callback-action:0.1\">");
        elem.append("<url>tcp://localhost:" + freeJMSPort + "?qu=fooqueue</url>");
        elem.append("<method>QUEUE_OFFER</method>");
        elem.append("<arg><property>");
        elem.append("<name>fookey</name><value>foovalue</value>");
        elem.append("</property></arg>");
        elem.append("</callback>");
        CallbackActionExecutor callbackActionExecutor = new CallbackActionExecutor();
        Context context = createContext("callback-action");
        try {
            callbackActionExecutor.sendNotification(context, XmlUtils.parseXml(elem.toString()));
            fail("Queue information was not missing, Should have thrown the exception");
        } catch (Exception ex) {
            assertEquals(WorkflowAction.Status.ERROR.toString(), context.getAction().getExternalStatus());
            assertEquals("IOException", ((ActionExecutorException) ex).getErrorCode());
            assertEquals("IOException: Invalid JMS URL, [queue/topic] is missing", ex.getMessage());
        }
    }

    private int findFreePort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            int port = socket.getLocalPort();
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore IOException on close()
            }
            return port;
        } catch (IOException e) {
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                }
            }
        }
        throw new IllegalStateException("Could not find a free port ");
    }

    @Override
    protected void tearDown() throws Exception {
        httpServer.stop();
        broker.stop();
    }

}
