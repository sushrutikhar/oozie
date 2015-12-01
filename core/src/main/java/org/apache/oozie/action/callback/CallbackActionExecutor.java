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

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.CallbackActionService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.Pair;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;

import javax.jms.JMSException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Callback action executor: It sends notification along with relevant arguments to applicable end point.
 */
public class CallbackActionExecutor extends ActionExecutor{

    public static final String ACTION_TYPE = "callback";
    public static final String NAME = "name";
    public static final String VALUE = "value";
    public static final String CALLBACK_ACTION_KEEPALIVE_SECS = "oozie.action.callback.keepalive.secs";
    private static final String CALLBACK_ACTION_HTTP_CONNECTION_TIMEOUT_SECS = "oozie.action.callback.http.connection.timeout.secs";
    private static final String CALLBACK_ACTION_HTTP_SOCKET_TIMEOUT_SECS = "oozie.action.callback.http.socket.timeout.secs";

    public enum METHOD {
        HTTP_GET, HTTP_POST, HTTP_PUT, QUEUE_OFFER, TOPIC_PUBLISH
    }

    protected CallbackActionExecutor() {
        super(ACTION_TYPE);
    }

    @Override
    public void initActionType() {
        super.initActionType();
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        try {
            context.setStartData("-", "-", "-");
            Element actionXml = XmlUtils.parseXml(action.getConf());
            sendNotification(context, actionXml);
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    protected void sendNotification(Context context, Element element) throws ActionExecutorException{
        Namespace ns = element.getNamespace();
        String urlString = element.getChild("url", ns).getText();
        METHOD method = METHOD.valueOf(element.getChild("method", ns).getText());
        List<Pair<String,String>> argList = fetchArgList(element.getChild("arg", ns), ns);
        Element captureOutput = element.getChild("capture-output", ns);
        if(method == METHOD.QUEUE_OFFER || method == METHOD.TOPIC_PUBLISH) {
            sendJMSNotification(context, urlString, method, argList);
            context.setExecutionData("OK", null);
        } else {
            HttpResponse response = sendHTTPNotification(context, urlString, method, argList);
            if(captureOutput != null) {
                Properties properties = new Properties();
                properties.setProperty("reason-phrase", response.getStatusLine().getReasonPhrase());
                properties.setProperty("status-code", String.valueOf(response.getStatusLine().getStatusCode()));
                context.setExecutionData("OK", properties);
            } else {
                context.setExecutionData("OK", null);
            }
        }
    }

    private List<Pair<String, String>> fetchArgList(Element arg, Namespace ns) {
        List<Pair<String,String>> properties = new ArrayList<Pair<String, String>>();
        List<Element> elementList = arg.getChildren("property", ns);
        for(Element property : elementList) {
            properties.add(new Pair<String, String>(property.getChild(NAME, ns).getValue(),
                    property.getChild(VALUE, ns).getValue()));
        }
        return properties;
    }

    protected HttpResponse sendHTTPNotification(Context context, String urlString, METHOD method,
                                                List<Pair<String,String>> argumentList)
            throws ActionExecutorException {
        HttpResponse response;
        try {
            if (method == METHOD.HTTP_GET) {
                response = httpGetMethod(urlString, argumentList);
            } else {
                response = httpPostAndPutMethod(method, urlString, argumentList);
            }
        } catch (IOException ex) {
            context.setExecutionData(WorkflowAction.Status.FAILED.toString(), null);
            throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED,
                    "CB1001", "Error during sending HTTP request", ex);
        }

        if (response.getStatusLine().getStatusCode() >= 400) {
            context.setExecutionData(WorkflowAction.Status.FAILED.toString(), null);
            throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED,
                    "CB1001", "Status code : " + response.getStatusLine().getStatusCode() +
                    ", Reason : " + response.getStatusLine().getReasonPhrase());
        }
        return response;
    }

    protected HttpResponse httpGetMethod(String urlString, List<Pair<String,String>> argumentList) throws IOException {
        HttpClient client = getHttpClient();
        List<NameValuePair> params = new ArrayList<NameValuePair>();
        for (Pair pair : argumentList) {
            params.add(new BasicNameValuePair(pair.getFist().toString(), pair.getSecond().toString()));
        }
        URI uri;
        try {
            uri = new URI( urlString + "?" + URLEncodedUtils.format(params, "utf-8"));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
        HttpGet httpGet = new HttpGet(uri);
        HttpResponse response = null;
        try {
            response = client.execute(httpGet);
        } finally {
            httpGet.releaseConnection();
        }
        return response;
    }

    protected HttpResponse httpPostAndPutMethod(METHOD method, String urlString,
                                          List<Pair<String,String>> argumentList) throws IOException {
        HttpClient client = getHttpClient();
        HttpEntityEnclosingRequestBase requestBase;
        if (method == METHOD.HTTP_POST) {
            requestBase = new HttpPost(urlString);
        } else {
            requestBase = new HttpPut(urlString);
        }
        List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
        for(Pair pair : argumentList) {
            urlParameters.add(new BasicNameValuePair(pair.getFist().toString(),
                    pair.getSecond().toString()));
        }
        try {
            requestBase.setEntity(new UrlEncodedFormEntity(urlParameters));
        } catch (UnsupportedEncodingException ex) {
            throw new IOException(ex);
        }
        HttpResponse response = null;
        try {
            response = client.execute(requestBase);
        } finally {
            requestBase.releaseConnection();
        }
        return response;
    }

    private HttpClient getHttpClient() {
        CallbackActionService callbackActionService = Services.get().get(CallbackActionService.class);
        HttpClient client = new DefaultHttpClient(callbackActionService.getConnectionManager());
        client.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, ConfigurationService
                .getInt(CallbackActionExecutor.CALLBACK_ACTION_HTTP_CONNECTION_TIMEOUT_SECS) * 1000);
        client.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, ConfigurationService
                .getInt(CallbackActionExecutor.CALLBACK_ACTION_HTTP_SOCKET_TIMEOUT_SECS) * 1000);
        client.getParams().setParameter(CoreConnectionPNames.SO_KEEPALIVE, ConfigurationService
                .getInt(CallbackActionExecutor.CALLBACK_ACTION_KEEPALIVE_SECS) * 1000);
        return client;
    }

    protected void sendJMSNotification(Context context, String urlString, METHOD method,
                                     List<Pair<String, String>> argumentList) throws ActionExecutorException {
        String[] url;
        try {
            url = decodeJMSURL(urlString);
        } catch (IOException ex) {
            context.setExecutionData(WorkflowAction.Status.ERROR.toString(), null);
            throw convertException(ex);
        }

        JMSNotification jmsNotification = null;
        try {
            jmsNotification = new JMSNotification(method, url[0], url[1]);
            jmsNotification.send(argumentList);
        } catch (Exception ex) {
            context.setExecutionData(WorkflowAction.Status.FAILED.toString(), null);
            throw convertException(ex);
        } finally {
            closeJMSConnection(context, jmsNotification);
        }
    }

    private void closeJMSConnection(Context context, JMSNotification jmsNotification) throws ActionExecutorException {
        try {
            if (jmsNotification != null) {
                jmsNotification.close();
            }
        } catch (JMSException ex) {
            context.setExecutionData(WorkflowAction.Status.FAILED.toString(), null);
            throw convertException(ex);
        }
    }

    private String[] decodeJMSURL(String urlString) throws IOException {
        String[] urlArray;
            urlArray = urlString.split("[?]");
            if (urlArray.length == 2) {
                if (urlArray[1].startsWith("queue=") || urlArray[1].startsWith("topic=")) {
                    urlArray[1] = urlArray[1].split("=")[1];
                } else {
                    throw new IOException("Invalid JMS URL, [queue/topic] is missing");
                }
            } else {
                throw new IOException( "Invalid JMS URL");
            }
        return urlArray;
    }

    @Override
    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        String externalStatus = action.getExternalStatus();
        WorkflowAction.Status status = externalStatus.equals("OK") ? WorkflowAction.Status.OK :
                WorkflowAction.Status.ERROR;
        context.setEndData(status, getActionSignal(status));
    }

    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
    }

    @Override
    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {

    }

    @Override
    public boolean isCompleted(String externalStatus) {
        return true;
    }
}
