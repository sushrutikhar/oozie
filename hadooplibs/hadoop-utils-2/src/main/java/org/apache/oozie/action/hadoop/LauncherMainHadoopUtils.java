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

package org.apache.oozie.action.hadoop;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.log4j.Logger;

public class LauncherMainHadoopUtils {
    private static Logger logger = Logger.getLogger("LauncherMainHadoopUtils");

    private LauncherMainHadoopUtils() {
    }

    private static Set<ApplicationId> getChildYarnJobs(Configuration actionConf) {
        System.out.println("Fetching child yarn jobs");
        Set<ApplicationId> childYarnJobs = new HashSet<ApplicationId>();
        if (actionConf.get("mapreduce.job.tags") == null) {
            logger.warn("Could not find Yarn tags property (mapreduce.job.tags)");
            return childYarnJobs;
        }
        String tag = actionConf.get("mapreduce.job.tags");
        System.out.println( "tag id : " + tag);
        GetApplicationsRequest gar = GetApplicationsRequest.newInstance();
        gar.setScope(ApplicationsRequestScope.OWN);
        gar.setApplicationTags(Collections.singleton(tag));
        try {
            ApplicationClientProtocol proxy = ClientRMProxy.createRMProxy(actionConf, ApplicationClientProtocol.class);
            GetApplicationsResponse apps = proxy.getApplications(gar);
            List<ApplicationReport> appsList = apps.getApplicationList();
            for(ApplicationReport appReport : appsList) {
                childYarnJobs.add(appReport.getApplicationId());
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Exception occurred while finding child jobs", ioe);
        } catch (YarnException ye) {
            throw new RuntimeException("Exception occurred while finding child jobs", ye);
        }
        System.out.println(childYarnJobs.size() + " Child yarn jobs are found");
        return childYarnJobs;
    }

    public static String getYarnJobForMapReduceAction(Configuration actionConf) {
        Set<ApplicationId> childYarnJobs = getChildYarnJobs(actionConf);
        String childJobId = null;
        if (!childYarnJobs.isEmpty()) {
            ApplicationId childJobYarnId = childYarnJobs.iterator().next();
            System.out.println("Found Map-Reduce job [" + childJobYarnId + "] already running");
            // Need the JobID version for Oozie
            childJobId = TypeConverter.fromYarn(childJobYarnId).toString();
        }
        return childJobId;
    }

    public static void killChildYarnJobs(Configuration actionConf) {
        try {
            Set<ApplicationId> childYarnJobs = getChildYarnJobs(actionConf);
            if (!childYarnJobs.isEmpty()) {
                System.out.println();
                System.out.println("Found [" + childYarnJobs.size() + "] Map-Reduce jobs from this launcher");
                System.out.println("Killing existing jobs and starting over:");
                YarnClient yarnClient = YarnClient.createYarnClient();
                yarnClient.init(actionConf);
                yarnClient.start();
                for (ApplicationId app : childYarnJobs) {
                    System.out.print("Killing job [" + app + "] ... ");
                    yarnClient.killApplication(app);
                    System.out.println("Done");
                }
                System.out.println();
            }
        } catch (YarnException ye) {
            throw new RuntimeException("Exception occurred while killing child job(s)", ye);
        } catch (IOException ioe) {
            throw new RuntimeException("Exception occurred while killing child job(s)", ioe);
        }
    }
}
