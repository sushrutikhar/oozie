package org.apache.oozie.command.coord;

import java.util.Date;
import java.util.List;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.SLAStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XmlUtils;
import org.jdom.JDOMException;

public class TestCoordELExtensions extends XTestCase{
    protected Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testCoordELActionMater() throws Exception {
        String jobId = "0000000-" + new Date().getTime() + "-testCoordELActionMater-C";

        Date startTime = DateUtils.parseDateUTC("2009-03-06T010:00Z");
        Date endTime = DateUtils.parseDateUTC("2009-03-11T10:00Z");
        addRecordToJobTable(jobId, startTime, endTime);
        new CoordActionMaterializeCommand(jobId, startTime, endTime).call();
        CoordinatorActionBean action = checkCoordAction(jobId + "@1");
    }

    protected CoordinatorActionBean checkCoordAction(String actionId) throws StoreException {
        CoordinatorStore store = new CoordinatorStore(false);
        try {
            CoordinatorActionBean action = store.getCoordinatorAction(actionId, false);
            SLAStore slaStore = new SLAStore(store);
            long lastSeqId[] = new long[1];
            List<SLAEventBean> slaEvents = slaStore.getSLAEventListNewerSeqLimited(0, 10, lastSeqId);
            // System.out.println("AAA " + slaEvents.size() + " : " +
            // lastSeqId[0]);
            if (slaEvents.size() == 0) {
                fail("Unable to GET any record of sequence id greater than 0");
            }
            return action;
        }
        catch (StoreException se) {
            se.printStackTrace();
            fail("Action ID " + actionId + " was not stored properly in db");
        }
        return null;
    }

    private void addRecordToJobTable(String jobId, Date startTime, Date endTime) throws StoreException {
        CoordinatorStore store = new CoordinatorStore(false);
        CoordinatorJobBean coordJob = new CoordinatorJobBean();
        coordJob.setId(jobId);
        coordJob.setAppName("testApp");
        coordJob.setStartTime(startTime);
        coordJob.setEndTime(endTime);
        coordJob.setTimeUnit(Timeunit.DAY);
        coordJob.setAppPath("testAppPath");
        coordJob.setStatus(CoordinatorJob.Status.PREMATER);
        coordJob.setCreatedTime(new Date()); // TODO: Do we need that?
        coordJob.setLastModifiedTime(new Date());
        coordJob.setUser("testUser");
        coordJob.setGroup("testGroup");
        coordJob.setTimeZone("America/Los_Angeles");
        String confStr = "<configuration></configuration>";
        coordJob.setConf(confStr);
        String appXml = "<coordinator-app xmlns='uri:oozie:coordinator:0.1' xmlns:sla='uri:oozie:sla:0.1' name='NAME' frequency=\"1\" start='2009-03-06T010:00Z' end='2009-03-11T10:00Z' timezone='America/Los_Angeles' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<controls>";
        appXml += "<timeout>10</timeout>";
        appXml += "<concurrency>2</concurrency>";
        appXml += "<execution>LIFO</execution>";
        appXml += "</controls>";
        appXml += "<input-events>";
        appXml += "<data-in name='A' dataset='a'>";
        appXml += "<dataset name='a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${MONTH}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<start-instance>${coordext:currentMonth(0,0,0)}</start-instance>";
        appXml += "<end-instance>${coordext:today(0,0)}</end-instance>";
        appXml += "</data-in>";
        appXml += "</input-events>";
        appXml += "<output-events>";
        appXml += "<data-out name='LOCAL_A' dataset='local_a'>";
        appXml += "<dataset name='local_a' frequency='7' initial-instance='2009-02-01T01:00Z' timezone='UTC' freq_timeunit='DAY' end_of_duration='NONE'>";
        appXml += "<uri-template>file:///tmp/coord/workflows/${YEAR}/${DAY}</uri-template>";
        appXml += "</dataset>";
        appXml += "<instance>${coordext:today(0,0)}</instance>";
        appXml += "</data-out>";
        appXml += "</output-events>";
        appXml += "<action>";
        appXml += "<workflow>";
        appXml += "<app-path>hdfs:///tmp/workflows/</app-path>";
        appXml += "<configuration>";
        appXml += "<property>";
        appXml += "<name>inputA</name>";
        appXml += "<value>${coord:dataIn('A')}</value>";
        appXml += "</property>";
        appXml += "<property>";
        appXml += "<name>inputB</name>";
        appXml += "<value>${coord:dataOut('LOCAL_A')}</value>";
        appXml += "</property>";
        appXml += "</configuration>";
        appXml += "</workflow>";
        appXml += " <sla:info>"
                // + " <sla:client-id>axonite-blue</sla:client-id>"
                + " <sla:app-name>test-app</sla:app-name>"
                + " <sla:nominal-time>${coord:nominalTime()}</sla:nominal-time>"
                + " <sla:should-start>5</sla:should-start>"
                + " <sla:should-end>120</sla:should-end>"
                + " <sla:notification-msg>Notifying User for ${coord:nominalTime()} nominal time </sla:notification-msg>"
                + " <sla:alert-contact>abc@yahoo.com</sla:alert-contact>"
                + " <sla:dev-contact>abc@yahoo.com</sla:dev-contact>"
                + " <sla:qa-contact>abc@yahoo.com</sla:qa-contact>" + " <sla:se-contact>abc@yahoo.com</sla:se-contact>"
                + "</sla:info>";
        appXml += "</action>";
        appXml += "</coordinator-app>";
        try {
            System.out.println(XmlUtils.prettyPrint(XmlUtils.parseXml(appXml)));
            ;
        }
        catch (JDOMException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        coordJob.setJobXml(appXml);
        coordJob.setLastActionNumber(0);
        coordJob.setFrequency(1);
        try {
            coordJob.setEndTime(DateUtils.parseDateUTC("2009-03-11T10:00Z"));
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail("Could not set end time");
        }
        try {
            store.beginTrx();
            store.insertCoordinatorJob(coordJob);
            store.commitTrx();
        }
        catch (StoreException se) {
            se.printStackTrace();
            store.rollbackTrx();
            fail("Unable to insert the test job record to table");
            throw se;
        }
        finally {
            store.closeTrx();
        }
    }
}
