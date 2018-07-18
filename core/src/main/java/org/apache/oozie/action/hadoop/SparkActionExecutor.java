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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.SparkConfigurationService;
import org.jdom.Element;
import org.jdom.Namespace;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SparkActionExecutor extends JavaActionExecutor {
    public static final String SPARK_MAIN_CLASS_NAME_1 = "org.apache.oozie.action.hadoop.SparkMain";
    public static final String SPARK_MAIN_CLASS_NAME_2 = "org.apache.oozie.action.hadoop.SparkMain2";
    public static final String SPARK_MAIN_CLASS_NAME_LATEST = "org.apache.oozie.action.hadoop.SparkMainLatest";

    public static final String TASK_USER_PRECEDENCE = "mapreduce.task.classpath.user.precedence"; // hadoop-2
    public static final String TASK_USER_CLASSPATH_PRECEDENCE = "mapreduce.user.classpath.first";  // hadoop-1
    public static final String SPARK_MASTER = "oozie.spark.master";
    public static final String SPARK_MODE = "oozie.spark.mode";
    public static final String SPARK_OPTS = "oozie.spark.spark-opts";
    public static final String SPARK_JOB_NAME = "oozie.spark.name";
    public static final String SPARK_CLASS = "oozie.spark.class";
    public static final String SPARK_JAR = "oozie.spark.jar";
    public static final String MAPRED_CHILD_ENV = "mapred.child.env";
    private static final String CONF_OOZIE_SPARK_SETUP_HADOOP_CONF_DIR = "oozie.action.spark.setup.hadoop.conf.dir";

    private String sparkVersion;

    public SparkActionExecutor() {
        super("spark");
    }

    @Override
    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath)
            throws ActionExecutorException {
        actionConf = super.setupActionConf(actionConf, context, actionXml, appPath);
        Namespace ns = actionXml.getNamespace();

        String master = actionXml.getChildTextTrim("master", ns);
        actionConf.set(SPARK_MASTER, master);

        String mode = actionXml.getChildTextTrim("mode", ns);
        if (mode != null) {
            actionConf.set(SPARK_MODE, mode);
        }

        String jobName = actionXml.getChildTextTrim("name", ns);
        actionConf.set(SPARK_JOB_NAME, jobName);

        String sparkClass = actionXml.getChildTextTrim("class", ns);
        if (sparkClass != null) {
            actionConf.set(SPARK_CLASS, sparkClass);
        }

        String jarLocation = actionXml.getChildTextTrim("jar", ns);
        actionConf.set(SPARK_JAR, jarLocation);

        StringBuilder sparkOptsSb = new StringBuilder();
        if (master.startsWith("yarn")) {
            String resourceManager = actionConf.get(HADOOP_JOB_TRACKER);
            Map<String, String> sparkConfig = Services.get().get(SparkConfigurationService.class).getSparkConfig(resourceManager);
            for (Map.Entry<String, String> entry : sparkConfig.entrySet()) {
                sparkOptsSb.append("--conf ").append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
            }
        }
        String sparkOpts = actionXml.getChildTextTrim("spark-opts", ns);
        if (sparkOpts != null) {
            sparkOptsSb.append(sparkOpts);
        }
        if (sparkOptsSb.length() > 0) {
            actionConf.set(SPARK_OPTS, sparkOptsSb.toString().trim());
        }

        sparkVersion = actionXml.getChildTextTrim("version" ,ns);

        // Setting if SparkMain should setup hadoop config *-site.xml
        boolean setupHadoopConf = actionConf.getBoolean(CONF_OOZIE_SPARK_SETUP_HADOOP_CONF_DIR,
                ConfigurationService.getBoolean(CONF_OOZIE_SPARK_SETUP_HADOOP_CONF_DIR));
        actionConf.setBoolean(CONF_OOZIE_SPARK_SETUP_HADOOP_CONF_DIR, setupHadoopConf);
        return actionConf;
    }

    @Override
    JobConf createLauncherConf(FileSystem actionFs, Context context, WorkflowAction action, Element actionXml,
                               Configuration actionConf) throws ActionExecutorException {

        JobConf launcherJobConf = super.createLauncherConf(actionFs, context, action, actionXml, actionConf);
        if (launcherJobConf.get("oozie.launcher." + TASK_USER_PRECEDENCE) == null) {
            launcherJobConf.set(TASK_USER_PRECEDENCE, "true");
        }
        if (launcherJobConf.get("oozie.launcher." + TASK_USER_CLASSPATH_PRECEDENCE) == null) {
            launcherJobConf.set(TASK_USER_CLASSPATH_PRECEDENCE, "true");
        }
        return launcherJobConf;
    }

    @Override
    Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath, Context context)
            throws ActionExecutorException {
        super.setupLauncherConf(conf, actionXml, appPath, context);

        // Set SPARK_HOME environment variable on launcher job
        // It is needed since pyspark client checks for it.
        String sparkHome = "SPARK_HOME=.";
        String mapredChildEnv = conf.get("oozie.launcher." + MAPRED_CHILD_ENV);

        if (mapredChildEnv == null) {
            conf.set(MAPRED_CHILD_ENV, sparkHome);
            conf.set("oozie.launcher." + MAPRED_CHILD_ENV, sparkHome);
        } else if (!mapredChildEnv.contains("SPARK_HOME")) {
            conf.set(MAPRED_CHILD_ENV, mapredChildEnv + "," + sparkHome);
            conf.set("oozie.launcher." + MAPRED_CHILD_ENV, mapredChildEnv + "," + sparkHome);
        }
        return conf;
    }

    @Override
    public List<Class> getLauncherClasses() {
        List<Class> classes = new ArrayList<Class>();
        try {
            if("0".equals(sparkVersion)){
                classes.add(Class.forName(SPARK_MAIN_CLASS_NAME_LATEST));
            } else if("2".equals(sparkVersion)){
                classes.add(Class.forName(SPARK_MAIN_CLASS_NAME_2));
            } else {
                classes.add(Class.forName(SPARK_MAIN_CLASS_NAME_1));
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found", e);
        }
        return classes;
    }


    /**
     * Return the sharelib name for the action.
     *
     * @param actionXml
     * @return returns <code>spark</code>.
     */
    @Override
    protected String getDefaultShareLibName(Element actionXml) {
        if("0".equals(sparkVersion)) {
            return "sparkLatest";
        } else if("2".equals(sparkVersion)) {
            return "spark2";
        } else{
            return "spark";
        }
    }

    @Override
    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        if ("0".equals(sparkVersion)) {
            return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, SPARK_MAIN_CLASS_NAME_LATEST);
        } else if("2".equals(sparkVersion)){
            return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, SPARK_MAIN_CLASS_NAME_2);
        } else {
            return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, SPARK_MAIN_CLASS_NAME_1);
        }
    }
}
