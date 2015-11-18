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

import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.util.Pair;

import java.util.Hashtable;
import java.util.List;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MapMessage;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;


public class JMSNotification
{
    // Defines the JNDI context factory.
    public final static String JNDI_FACTORY="oozie.action.callback.jms.naming.factory.initial";

    // Defines the JMS context factory.
    public final static String JMS_FACTORY="ConnectionFactory";

    private ConnectionFactory conFactory;
    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private InitialContext initialContext;
    private MapMessage msg;

    /**
     * Creates all the necessary objects for sending
     * messages to a JMS queue.
     *
     * @param jmsMethod it can be QUEUE_OFFER or TOPIC_PUBLISH
     * @param url url
     * @param producerName name of queue/topic
     * @exception NamingException if operation cannot be performed
     * @exception JMSException if JMS fails to initialize due to internal error
     */
    public JMSNotification(CallbackActionExecutor.METHOD jmsMethod, String url, String producerName)
            throws NamingException, JMSException {
        initialContext = getInitialContext(url);
        conFactory = (ConnectionFactory) initialContext.lookup(JMS_FACTORY);
        connection = conFactory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        if (jmsMethod == CallbackActionExecutor.METHOD.QUEUE_OFFER) {
            producer = session.createProducer(session.createQueue(producerName));
        } else {
            producer = session.createProducer(session.createTopic(producerName));
        }
        producer.setTimeToLive(ConfigurationService
                .getInt(CallbackActionExecutor.CALLBACK_ACTION_KEEPALIVE_SECS) * 1000);
        msg = session.createMapMessage();
        connection.start();
    }

    /**
     * Sends a message to a JMS queue/topic
     *
     * @param argumentList  message to be sent
     * @exception JMSException if JMS fails to send message due to internal error
     */
    public void send(List<Pair<String, String>> argumentList) throws JMSException {
        for (Pair pair : argumentList) {
            msg.setString(pair.getFist().toString(),pair.getSecond().toString());
        }
        producer.send(msg);
    }

    /**
     * Closes JMS objects.
     * @exception JMSException if JMS fails to close objects due to internal error
     */
    public void close() throws JMSException {
        producer.close();
        session.close();
        connection.close();
    }

    private InitialContext getInitialContext(String host) throws NamingException {
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, ConfigurationService.get(JNDI_FACTORY));
        env.put(Context.PROVIDER_URL, host);
        return new InitialContext(env);
    }
}