package icecube.daq.reqFiller.test;

import icecube.daq.payload.IPayload;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.PayloadDestination;
import icecube.daq.payload.splicer.Payload;
import icecube.daq.reqFiller.RequestFiller;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

class TestFiller
    extends RequestFiller
{
    TestFiller(boolean sendEmptyPayloads)
    {
        super("TestFiller", sendEmptyPayloads);
    }

    /**
     * Compare request and data payload.
     *
     * @param reqPayload request payload
     * @param dataPayload data payload
     *
     * @return <tt>-1</tt> if data is before request,
     *         <tt>0</tt> if data is within request
     *         <tt>1</tt> if data is after request
     */
    public int compareRequestAndData(IPayload reqPayload,
                                     IPayload dataPayload)
    {
        System.err.println("CMP REQ "+reqPayload+" AND "+dataPayload);
        return 0;
    }

    /**
     * Dispose of a data payload which is no longer needed.
     *
     * @param data payload
     */
    public void disposeData(IPayload data)
    {
        // do nothing
    }

    /**
     * Dispose of a list of data payloads which are no longer needed.
     *
     * @param list list of data payload
     */
    public void disposeDataList(List dataList)
    {
        // do nothing
    }

    /**
     * Perform any necessary clean-up after fulfillment thread exits.
     */
    public void finishThreadCleanup()
    {
        // do nothing
    }

    /**
     * Does this data payload match one of the request criteria?
     *
     * @param reqPayload request
     * @param dataPayload data
     *
     * @return <tt>true</tt> if the data payload is part of the
     *         current request
     */
    public boolean isRequested(IPayload reqPayload, IPayload dataPayload)
    {
        return true;
    }

    /**
     * Make a payload out of the request in req and the data in dataList.
     *
     * @param reqPayload request
     * @param dataList list of data payloads matching the request
     *
     * @return The payload created for the current request.
     */
    public IPayload makeDataPayload(IPayload reqPayload, List dataList)
    {
        System.err.println("Faking stupid Output payload");
        return new TestData(666);
    }

    /**
     * Recycle payloads left after the final output payload.
     */
    public void recycleFinalData()
    {
        // do nothing
    }

    /**
     * Send the output payload.
     *
     * @param payload payload to send
     *
     * @return <tt>false</tt> if payload could not be sent
     */
    public boolean sendOutput(IPayload payload)
    {
        // do nothing
        return true;
    }

    /**
     * Set request start/end times.
     *
     * @param payload current request
     */
    public void setRequestTimes(IPayload payload)
    {
        // do nothing
    }

    public String toString()
    {
        return "XXX";
    }
}

class TestData
    extends Payload
{
    private int val;

    TestData(int val)
    {
        super();

        this.val = val;
    }

    public Object deepCopy()
    {
        throw new Error("Unimplemented");
    }

    public int getPayloadLength()
    {
        throw new Error("Unimplemented");
    }

    public int getPayloadType()
    {
        throw new Error("Unimplemented");
    }

    public int getPayloadInterfaceType()
    {
        throw new Error("Unimplemented");
    }

    public IUTCTime getPayloadTimeUTC()
    {
        throw new Error("Unimplemented");
    }

    public void loadPayload()
    {
        // do nothing
    }

    public int writePayload(PayloadDestination dest)
    {
        throw new Error("Unimplemented");
    }

    public int writePayload(int offset, ByteBuffer buf)
    {
        throw new Error("Unimplemented");
    }

    public String toString()
    {
        return "TestData["+val+"] "+super.toString();
    }
}

public class RequestFillerTest
    extends TestCase
{
    /**
     * Constructs and instance of this test.
     *
     * @param name the name of the test.
     */
    public RequestFillerTest(String name)
    {
        super(name);
    }

    public void testAddData()
    {
        TestFiller tf = new TestFiller(true);

        for (int i = 0; i < 10; i++) {
            tf.addData(new TestData(i));

            assertEquals("Unexpected number of data elements queued",
                         i + 1, tf.getNumDataPayloadsQueued());
            assertEquals("Unexpected number of data elements received",
                         i + 1, tf.getNumDataPayloadsReceived());
        }
    }

    public void testAddDataList()
    {
        ArrayList list = new ArrayList();

        for (int i = 0; i < 10; i++) {
            list.add(new TestData(i));

            TestFiller tf = new TestFiller(true);
            tf.addData(list, 0);
            assertEquals("Unexpected number of data elements queued",
                         i + 1, tf.getNumDataPayloadsQueued());
            assertEquals("Unexpected number of data elements received",
                         i + 1, tf.getNumDataPayloadsReceived());
        }

        TestFiller tf = new TestFiller(true);

        tf.addData(list, list.size());
        assertEquals("Unexpected number of data elements queued",
                     0, tf.getNumDataPayloadsQueued());
        assertEquals("Unexpected number of data elements received",
                     0, tf.getNumDataPayloadsReceived());

        tf.addData(list, list.size() + 99);
        assertEquals("Unexpected number of data elements queued",
                     0, tf.getNumDataPayloadsQueued());
        assertEquals("Unexpected number of data elements received",
                     0, tf.getNumDataPayloadsReceived());

        final int numAdded = list.size() / 2;

        tf.addData(list, list.size() - numAdded);
        assertEquals("Unexpected number of data elements queued",
                     numAdded, tf.getNumDataPayloadsQueued());
        assertEquals("Unexpected number of data elements received",
                     numAdded, tf.getNumDataPayloadsReceived());

        tf.addData(list, -100);
        assertEquals("Unexpected number of data elements queued",
                     numAdded + list.size(), tf.getNumDataPayloadsQueued());
        assertEquals("Unexpected number of data elements received",
                     numAdded + list.size(), tf.getNumDataPayloadsReceived());
    }

    public void testAddDataStop()
    {
        TestFiller tf = new TestFiller(true);

        for (int i = 0; i < 10; i++) {
            tf.addDataStop();

            assertEquals("Unexpected number of data elements queued",
                         i + 1, tf.getNumDataPayloadsQueued());
            assertEquals("Unexpected number of data elements received",
                         0, tf.getNumDataPayloadsReceived());
        }
    }

    public void testAddRequest()
    {
        TestFiller tf = new TestFiller(true);

        for (int i = 0; i < 10; i++) {
            tf.addRequest(new TestData(i));

            assertEquals("Unexpected number of requests queued",
                         i + 1, tf.getNumRequestsQueued());
        }
    }

    public void testAddRequestStop()
    {
        TestFiller tf = new TestFiller(true);

        for (int i = 0; i < 10; i++) {
            tf.addRequestStop();

            assertEquals("Unexpected number of requests queued",
                         i + 1, tf.getNumRequestsQueued());
        }
    }

    public void testEmptyRun()
    {
        TestFiller tf = new TestFiller(true);
        assertFalse("Thread started by constructor", tf.isRunning());

        tf.startThread();
        assertTrue("Thread was not started", tf.isRunning());

        tf.stopThread();
        try {
            Thread.sleep(200);
        } catch (Exception ex) {
            // ignore
        }
        assertFalse("Thread was not stopped", tf.isRunning());
        assertEquals("Unexpected number of data elements queued",
                     0, tf.getNumDataPayloadsQueued());
        assertEquals("Unexpected number of requests queued",
                     0, tf.getNumRequestsQueued());
    }

    public void testExplicitStop()
    {
        TestFiller tf = new TestFiller(true);
        assertFalse("Thread started by constructor", tf.isRunning());

        tf.startThread();
        assertTrue("Thread was not started", tf.isRunning());

        tf.addDataStop();
        tf.addRequestStop();
        try {
            Thread.sleep(200);
        } catch (Exception ex) {
            // ignore
        }

        assertFalse("Thread was not stopped", tf.isRunning());
        assertEquals("Unexpected number of data elements queued",
                     0, tf.getNumDataPayloadsQueued());
        assertEquals("Unexpected number of requests queued",
                     0, tf.getNumRequestsQueued());
    }

    public void testOnlyRequests()
    {
        TestFiller tf = new TestFiller(true);
        assertFalse("Thread started by constructor", tf.isRunning());

        tf.startThread();
        assertTrue("Thread was not started", tf.isRunning());

        tf.addRequest(new TestData(0));

        tf.addDataStop();
        tf.addRequestStop();
        try {
            Thread.sleep(200);
        } catch (Exception ex) {
            // ignore
        }

        assertFalse("Thread was not stopped", tf.isRunning());
        assertEquals("Unexpected number of data elements queued",
                     0, tf.getNumDataPayloadsQueued());
        assertEquals("Unexpected number of requests queued",
                     0, tf.getNumRequestsQueued());
    }


    public void testOnlyData()
    {
        TestFiller tf = new TestFiller(true);
        assertFalse("Thread started by constructor", tf.isRunning());

        tf.startThread();
        assertTrue("Thread was not started", tf.isRunning());

        tf.addData(new TestData(0));

        tf.addDataStop();
        tf.addRequestStop();
        try {
            Thread.sleep(200);
        } catch (Exception ex) {
            // ignore
        }

        assertFalse("Thread was not stopped", tf.isRunning());
        assertEquals("Unexpected number of data elements queued",
                     0, tf.getNumDataPayloadsQueued());
        assertEquals("Unexpected number of requests queued",
                     0, tf.getNumRequestsQueued());
    }

    public void testOneReqAndData()
    {
        TestFiller tf = new TestFiller(true);
        assertFalse("Thread started by constructor", tf.isRunning());

        tf.startThread();
        assertTrue("Thread was not started", tf.isRunning());

        tf.addRequest(new TestData(0));
        tf.addData(new TestData(0));

        tf.addDataStop();
        tf.addRequestStop();
        try {
            Thread.sleep(200);
        } catch (Exception ex) {
            // ignore
        }

        assertFalse("Thread was not stopped", tf.isRunning());
        assertEquals("Unexpected number of data elements queued",
                     0, tf.getNumDataPayloadsQueued());
        assertEquals("Unexpected number of requests queued",
                     0, tf.getNumRequestsQueued());
    }

    public void testBasicGets()
    {
        TestFiller tf = new TestFiller(true);
        assertFalse("Thread started by constructor", tf.isRunning());

        assertEquals("Bad avg output data payloads",
                     0, tf.getAverageOutputDataPayloads());
        assertEquals("Bad back-end state",
                     "Unknown state", tf.getBackEndState());
        assertEquals("Bad back-end timing",
                     "NOT RUNNING", tf.getBackEndTiming());
        assertEquals("Bad data payloads/second",
                     0.0, tf.getDataPayloadsPerSecond(), 0.000001);
        assertEquals("Bad number of data payloads",
                     0, tf.getNumBadDataPayloads());
        assertEquals("Bad number of requests", 0, tf.getNumBadRequests());
        assertEquals("Bad number of data payloads cached",
                     0, tf.getNumDataPayloadsCached());
        assertEquals("Bad number of data payloads discarded",
                     0, tf.getNumDataPayloadsDiscarded());
        assertEquals("Bad number of data payloads queued",
                     0, tf.getNumDataPayloadsQueued());
        assertEquals("Bad number of data payloads received",
                     0, tf.getNumDataPayloadsReceived());
        assertEquals("Bad number of empty loops", 0, tf.getNumEmptyLoops());
        assertEquals("Bad number of null data payloads",
                     0, tf.getNumNullDataPayloads());
        assertEquals("Bad number of null output payloads",
                     0, tf.getNumNullOutputs());
        assertEquals("Bad number of failed output payloads",
                     0, tf.getNumOutputsFailed());
        assertEquals("Bad number of ignored output payloads",
                     0, tf.getNumOutputsIgnored());
        assertEquals("Bad number of sent output payloads",
                     0, tf.getNumOutputsSent());
        assertEquals("Bad number of requests dropped",
                     0, tf.getNumRequestsDropped());
        assertEquals("Bad number of requests queued",
                     0, tf.getNumRequestsQueued());
        assertEquals("Bad number of unused data payloads",
                     0, tf.getNumUnusedDataPayloads());
        assertEquals("Bad outputs/second",
                     0.0, tf.getOutputsPerSecond(), 0.000001);
        assertEquals("Bad total number of data payloads",
                     0, tf.getTotalBadDataPayloads());
        assertEquals("Bad total discarded data payloads",
                     0, tf.getTotalDataPayloadsDiscarded());
        assertEquals("Bad total received data payloads",
                     0, tf.getTotalDataPayloadsReceived());
        assertEquals("Bad total data stops received",
                     0, tf.getTotalDataStopsReceived());
        assertEquals("Bad total outputs failed",
                     0, tf.getTotalOutputsFailed());
        assertEquals("Bad total outputs ignored",
                     0, tf.getTotalOutputsIgnored());
        assertEquals("Bad total outputs sent",
                     0, tf.getTotalOutputsSent());
        assertEquals("Bad total request stops received",
                     0, tf.getTotalRequestStopsReceived());

        assertFalse("Thread was not stopped", tf.isRunning());
        assertEquals("Unexpected number of data elements queued",
                     0, tf.getNumDataPayloadsQueued());
        assertEquals("Unexpected number of requests queued",
                     0, tf.getNumRequestsQueued());
    }

    /**
     * Create test suite for this class.
     *
     * @return the suite of tests declared in this class.
     */
    public static Test suite()
    {
        return new TestSuite(RequestFillerTest.class);
    }

    /**
     * Main routine which runs text test in standalone mode.
     *
     * @param args the arguments with which to execute this method.
     */
    public static void main(String[] args)
    {
        TestRunner.run(suite());
    }
}
