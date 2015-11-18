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

package org.apache.oozie.service;

import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.oozie.executor.jpa.JPAExecutor;

/**
 * Service that manages JPA and executes {@link JPAExecutor}.
 */
@SuppressWarnings("deprecation")
public class CallbackActionService implements Service {

    public static final String POOL_SIZE = "oozie.action.callback.http.pool.size";
    private PoolingClientConnectionManager connectionManager;

    public PoolingClientConnectionManager getConnectionManager() {
        return connectionManager;
    }


    /**
     * Return the public interface of the service.
     *
     * @return {@link CallbackActionService}.
     */
    public Class<? extends Service> getInterface() {
        return CallbackActionService.class;
    }

    /**
     * Initializes the {@link CallbackActionService}.
     *
     * @param services services instance.
     */
    public void init(Services services) throws ServiceException {
        SchemeRegistry registry = new SchemeRegistry();
        registry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
        registry.register(new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));
        connectionManager = new PoolingClientConnectionManager(registry);
        connectionManager.setMaxTotal(ConfigurationService.getInt(POOL_SIZE));
    }

    /**
     * Destroy the CallbackActionService
     */
    public void destroy() {
        if (connectionManager != null) {
            connectionManager.shutdown();
        }
    }
}