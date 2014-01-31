package icecube.daq.reqFiller;

import icecube.daq.payload.ILoadablePayload;
import icecube.daq.payload.IPayload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Generic request fulfillment engine.
 */
public abstract class RequestFiller
{
    public static final ILoadablePayload DROPPED_PAYLOAD =
        new DummyPayload();

    /** Stop marker for request and data queues. */
    private static final StopMarker STOP_MARKER = StopMarker.INSTANCE;

    /** Message logger. */
    private static final Log LOG = LogFactory.getLog(RequestFiller.class);

    enum State {
        ERR_UNKNOWN("Unknown state"),
        ERR_NULL_DATA("Null data payload"),
        ERR_NULL_OUTPUT("Null hit"),
        ERR_BAD_REQUEST("Null request"),
        ERR_BAD_DATA("Null output"),

        STOP_REQUEST("Global request STOP received"),
        STOP_DATA("Splicer STOP received"),

        EMPTY_LOOP("Empty loop"),
        WAIT_REQUEST("Waiting for request"),
        WAIT_DATA("Waiting for data"),
        GOT_REQUEST("Got request"),
        GOT_DATA("Got data"),
        LOADED_REQUEST("Loaded request"),
        LOADED_DATA("Loaded data"),
        TOSSED_REQUEST("Threw away unused request"),
        TOSSED_DATA("Threw away unused data"),
        EARLY_DATA("Got early data"),
        SAVED_DATA("Data was saved"),
        OUTPUT_SENT("Sent output"),
        OUTPUT_FAILED("Could not send output"),
        OUTPUT_IGNORED("Ignored empty output");

        private String description;

        State(String description) {
            this.description = description;
        }

        String getDescription() { return description; }
    };

    /** Thread name. */
    private String threadName;
    /** <tt>true</tt> if empty output payloads should be sent. */
    private boolean sendEmptyPayloads;

    /** Request queue -- ACCESS MUST BE SYNCHRONIZED. */
    private List requestQueue = new LinkedList();
    /** Data queue -- ACCESS MUST BE SYNCHRONIZED. */
    private List dataQueue = new LinkedList();

    /** accumulator for data to be sent in next output payload. */
    private List requestedData = new ArrayList();

    /** Lock used to atomically update numOutputsSent and lastOutputTime */
    private Object outputDataLock = new Object();

    // per-run monitoring counters
    private long dataPerSecX100;
    private long firstOutputTime;
    private long lastOutputTime;
    private long numBadData;
    private long numBadRequests;
    private long numDataDiscarded;
    private long numDataFetched;
    private long numDataReceived;
    private long numDataUsed;
    private long numDroppedData;
    private long numDroppedRequests;
    private long numEmptyLoops;
    private long numNullData;
    private long numNullOutputs;
    private long numOutputsFailed;
    private long numOutputsIgnored;
    private long numOutputsSent;
    private long numReqFetched;
    private long numRequestsReceived;
    private long outputPerSecX100;
    private long reqsPerSecX100;

    // lifetime monitoring counters
    private long totBadData;
    private long totDataDiscarded;
    private long totDataReceived;
    private long totDataStops;
    private long totOutputsFailed;
    private long totOutputsIgnored;
    private long totOutputsSent;
    private long totRequestsReceived;
    private long totRequestStops;

    // ceiling for number of failed outputs
    private long maxOutputFailures = 10;

    // current state
    private State state = State.ERR_UNKNOWN;

    // time at start of year
    private long jan1Millis = Long.MIN_VALUE;

    private WorkerThread workerThread;

    /**
     * Create a request fulfillment engine.
     *
     * @param threadName name of fulfillment thread.
     * @param sendEmptyPayloads <tt>true</tt> if output payloads are sent
     *                          even if they don't contain any data payloads
     */
    public RequestFiller(String threadName, boolean sendEmptyPayloads)
    {
        this.threadName = threadName;
        this.sendEmptyPayloads = sendEmptyPayloads;
    }

    /**
     * Add data to data queue.
     *
     * @param newData list of new data
     * @param offset number of previously-seen data at front of list
     *
     * @throws IOException if the processing thread is stopped
     */
    public void addData(List newData, int offset)
        throws IOException
    {
        if (!isRunning() && LOG.isErrorEnabled()) {
            throw new IOException("Adding data while thread " + threadName +
                                  " is stopped");
        }

        // adjust offset to fit within legal bounds
        if (offset < 0) {
            offset = 0;
        } else if (offset > newData.size()) {
            offset = newData.size();
        }

        final int newLen = newData.size() - offset;

        synchronized (dataQueue) {
            for (int i = 0; i < newLen; i++) {
                dataQueue.add(newData.get(i + offset));
            }

            numDataReceived += newLen;
            totDataReceived += newLen;

            dataQueue.notify();
        }
    }

    /**
     * Add data to data queue.
     *
     * @param newData new data payload
     *
     * @throws IOException if the processing thread is stopped
     */
    public void addData(IPayload newData)
        throws IOException
    {
        if (!isRunning()) {
            throw new IOException("Adding data while thread " + threadName +
                                  " is stopped");
        }

        synchronized (dataQueue) {
            dataQueue.add(newData);

            numDataReceived++;
            totDataReceived++;

            dataQueue.notify();
        }
    }

    /**
     * Add stop marker to data queue.
     *
     * @throws IOException if the processing thread is stopped
     */
    public void addDataStop()
        throws IOException
    {
        if (!isRunning()) {
            throw new IOException("Adding data stop while thread " +
                                  threadName + " is stopped");
        }

        synchronized (dataQueue) {
            dataQueue.add(STOP_MARKER);
            dataQueue.notify();
        }
    }

    /**
     * Add request to request queue.
     *
     * @param newReq new request payload
     *
     * @throws IOException if the processing thread is stopped
     */
    public void addRequest(IPayload newReq)
        throws IOException
    {
        if (!isRunning()) {
            throw new IOException("Adding request while thread " + threadName +
                                  " is stopped");
        }

        synchronized (requestQueue) {
            requestQueue.add(newReq);

            numRequestsReceived++;
            totRequestsReceived++;

            requestQueue.notify();
        }
    }

    /**
     * Add stop marker to request queue.
     *
     * @throws IOException if the processing thread is stopped
     */
    public void addRequestStop()
        throws IOException
    {
        if (!isRunning()) {
            throw new IOException("Adding request stop while thread " +
                                  threadName + " is stopped");
        }

        synchronized (requestQueue) {
            requestQueue.add(STOP_MARKER);
            requestQueue.notify();
        }
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
    public abstract int compareRequestAndData(IPayload reqPayload,
                                              IPayload dataPayload);

    /**
     * Dispose of a data payload which is no longer needed.
     *
     * @param data payload
     */
    public abstract void disposeData(ILoadablePayload data);

    /**
     * Dispose of a list of data payloads which are no longer needed.
     *
     * @param dataList list of data payload
     */
    public abstract void disposeDataList(List dataList);

    /**
     * Perform any necessary clean-up after fulfillment thread exits.
     */
    public abstract void finishThreadCleanup();

    /**
     * Get average number of data payloads per output payload.
     *
     * @return data payloads/output payload
     */
    public long getAverageOutputDataPayloads()
    {
        if (numOutputsSent == 0) {
            return 0;
        }

        return numDataUsed / numOutputsSent;
    }

    /**
     * Get current rate of data payloads per second.
     *
     * @return data payloads/second
     */
    public double getDataPayloadsPerSecond()
    {
        return (double) dataPerSecX100 / 100.0;
    }

    public String getDebugMsg()
    {
        return workerThread.getDebugMsg();
    }

    /**
     * Get first output time.
     *
     * @return first output time
     */
    public long getFirstOutputTime()
    {
        return firstOutputTime;
    }

    /**
     * Get current state.
     *
     * @return state string
     */
    public String getInternalState()
    {
        return state.getDescription();
    }

    /**
     * Get internal timing profile.
     *
     * @return internal timing profile
     */
    public String getInternalTiming()
    {
        return (workerThread == null ? "NOT RUNNING" : "NOT AVAILABLE");
    }

    /**
     * Get last output time.
     *
     * @return last output time
     */
    public long getLastOutputTime()
    {
        return lastOutputTime;
    }

    /**
     * Compute the latency since the data from the last payload was created.
     *
     * @return latency in seconds
     */
    public double getLatency()
    {
        if (jan1Millis == Long.MIN_VALUE) {
            GregorianCalendar cal = new GregorianCalendar();
            final int year = cal.get(GregorianCalendar.YEAR);
            cal.set(year, 0, 1, 0, 0, 0);
            jan1Millis = cal.getTimeInMillis();
        }

        if (lastOutputTime <= 0) {
            return 0.0;
        }

        final long usecsSinceJan1 = System.currentTimeMillis() - jan1Millis;
        final long ticksSinceJan1 = usecsSinceJan1 * 10000000;
        final long latencyInTicks = ticksSinceJan1 - lastOutputTime;
        final double latencyInSeconds = latencyInTicks / 10000000000.0;

        return latencyInSeconds;
    }

    /**
     * Number of data payloads which could not be loaded.
     *
     * @return number of bad data payloads
     */
    public long getNumBadDataPayloads()
    {
        return numBadData;
    }

    /**
     * Number of requests which could not be loaded.
     *
     * @return number of bad requests
     */
    public long getNumBadRequests()
    {
        return numBadRequests;
    }

    /**
     * Get number of data payloads cached for next output payload
     *
     * @return number of cached data payloads
     */
    public int getNumDataPayloadsCached()
    {
        return requestedData.size();
    }

    /**
     * Get number of data payloads thrown away.
     *
     * @return number of data payloads thrown away
     */
    public long getNumDataPayloadsDiscarded()
    {
        return numDataDiscarded;
    }

    /**
     * Get number of data payloads dropped while stopping.
     *
     * @return number of data payloads dropped
     */
    public long getNumDataPayloadsDropped()
    {
        return numDroppedData;
    }

    /**
     * Get number of data payloads queued for processing.
     *
     * @return number of data payloads queued
     */
    public int getNumDataPayloadsQueued()
    {
        return dataQueue.size();
    }

    /**
     * Get number of data payloads received.
     *
     * @return number of data payloads received
     */
    public long getNumDataPayloadsReceived()
    {
        return numDataReceived;
    }

    /**
     * Get number of passes through the main loop without a request.
     *
     * @return number of empty loops
     */
    public long getNumEmptyLoops()
    {
        return numEmptyLoops;
    }

    /**
     * Get number of null data payloads received.
     *
     * @return number of null data payloads received
     */
    public long getNumNullDataPayloads()
    {
        return numNullData;
    }

    /**
     * Get number of output payloads which could not be created.
     *
     * @return number of null output payloads
     */
    public long getNumNullOutputs()
    {
        return numNullOutputs;
    }

    /**
     * Get number of outputs which could not be sent.
     *
     * @return number of failed outputs
     */
    public long getNumOutputsFailed()
    {
        return numOutputsFailed;
    }

    /**
     * Get number of ignored empty output payloads.
     *
     * @return number of ignored empty output payloads
     */
    public long getNumOutputsIgnored()
    {
        return numOutputsIgnored;
    }

    /**
     * Get number of outputs sent.
     *
     * @return number of outputs sent
     */
    public long getNumOutputsSent()
    {
        return numOutputsSent;
    }

    /**
     * Number of requests dropped while stopping.
     *
     * @return number of requests dropped
     */
    public long getNumRequestsDropped()
    {
        return numDroppedRequests;
    }

    /**
     * Get number of requests queued for processing.
     *
     * @return number of requests queued
     */
    public int getNumRequestsQueued()
    {
        return requestQueue.size();
    }

    /**
     * Get number of requests received.
     *
     * @return number of requests received
     */
    public long getNumRequestsReceived()
    {
        return numRequestsReceived;
    }

    /**
     * Get current rate of output payloads per second.
     *
     * @return outputs/second
     */
    public double getOutputsPerSecond()
    {
        return (double) outputPerSecX100 / 100.0;
    }

    /**
     * Get current rate of requests per second.
     *
     * @return requests/second
     */
    public double getRequestsPerSecond()
    {
        return (double) reqsPerSecX100 / 100.0;
    }

    /**
     * Get total number of unloadable data payloads since last reset
     *
     * @return total number of bad data payloads since last reset
     */
    public long getTotalBadDataPayloads()
    {
        return totBadData;
    }

    /**
     * Total number of data payloads thrown away since last reset.
     *
     * @return total number of data payloads thrown away since last reset
     */
    public long getTotalDataPayloadsDiscarded()
    {
        return totDataDiscarded;
    }

    /**
     * Total number of data payloads received since last reset.
     *
     * @return total number of data payloads received since last reset
     */
    public long getTotalDataPayloadsReceived()
    {
        return totDataReceived;
    }

    /**
     * Total number of stop messages received from the data source.
     *
     * @return total number of received stop messages
     */
    public long getTotalDataStopsReceived()
    {
        return totDataStops;
    }

    /**
     * Total number of unsendable output payloads since last reset.
     *
     * @return total number of failed output payloads
     */
    public long getTotalOutputsFailed()
    {
        return totOutputsFailed;
    }

    /**
     * Total number of ignored empty output payloads since last reset.
     *
     * @return total number of ignored empty output payloads
     */
    public long getTotalOutputsIgnored()
    {
        return totOutputsIgnored;
    }

    /**
     * Total number of output payloads sent since last reset.
     *
     * @return total number of output payloads sent since last reset.
     */
    public long getTotalOutputsSent()
    {
        return totOutputsSent;
    }

    /**
     * Total number of stop messages received from the request source.
     *
     * @return total number of received stop messages
     */
    public long getTotalRequestStopsReceived()
    {
        return totRequestStops;
    }

    /**
     * Total number of requests received.
     *
     * @return total number of requests received
     */
    public long getTotalRequestsReceived()
    {
        return totRequestsReceived;
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
    public abstract boolean isRequested(IPayload reqPayload,
                                        IPayload dataPayload);

    /**
     * Is the worker thread running?
     *
     * @return <tt>true</tt> if thread is running
     */
    public boolean isRunning()
    {
        return (workerThread != null);
    }

    /**
     * Make a payload out of the request in req and the data in dataList.
     *
     * @param reqPayload request
     * @param dataList list of data payloads matching the request
     *
     * @return The payload created for the current request.
     */
    public abstract ILoadablePayload makeDataPayload(IPayload reqPayload,
                                                     List dataList);

    /**
     * Recycle payloads left after the final output payload.
     */
    public abstract void recycleFinalData();

    /**
     * Reset the request filler after it has been stopped.
     */
    public void reset()
    {
        resetCounters();

        state = State.ERR_UNKNOWN;

        if (dataQueue.size() > 0) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Data payloads queued at " + threadName + " reset");
            }

            synchronized (dataQueue) {
                disposeDataList(dataQueue);
                dataQueue.clear();
            }
        }

        if (!isRunning()) {
            startThread();
        }
    }

    private void resetCounters()
    {
        dataPerSecX100 = 0;
        firstOutputTime = 0;
        lastOutputTime = 0;
        numBadData = 0;
        numBadRequests = 0;
        numDataDiscarded = 0;
        numDataFetched = 0;
        numDataReceived = 0;
        numDataUsed = 0;
        numDroppedData = 0;
        numDroppedRequests = 0;
        numEmptyLoops = 0;
        numNullData = 0;
        numNullOutputs = 0;
        numOutputsFailed = 0;
        numOutputsSent = 0;
        numReqFetched = 0;
        numRequestsReceived = 0;
        outputPerSecX100 = 0;
        reqsPerSecX100 = 0;
    }

    public long[] resetOutputData()
    {
        long[] data;
        synchronized (outputDataLock) {
            data = new long[] {
                numOutputsSent, firstOutputTime, lastOutputTime
            };

            synchronized (dataQueue) {
                synchronized (requestQueue) {
                    resetCounters();
                    numDataReceived = dataQueue.size();
                    numRequestsReceived = requestQueue.size();
                }
            }
        }
        return data;
    }

    /**
     * Send the output payload.
     *
     * @param payload payload to send
     *
     * @return <tt>false</tt> if payload could not be sent
     */
    public abstract boolean sendOutput(ILoadablePayload payload);

    /**
     * Set the maximum number of failed outputs permitted before the thread
     * is stopped.
     *
     * @param max maximum number of output failures allowed
     */
    public void setMaximumOutputFailures(long max)
    {
        maxOutputFailures = max;
    }

    /**
     * Set request start/end times.
     *
     * @param payload current request
     */
    public abstract void setRequestTimes(IPayload payload);

    /**
     * Start the processing thread.
     */
    public void startThread()
    {
        if (isRunning()) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Thread " + threadName + " already running!");
            }
        } else {
            workerThread = new WorkerThread(threadName);
            workerThread.start();
        }
    }

    /**
     * If the thread is running, stop it.
     */
    public void stopThread()
        throws IOException
    {
        if (isRunning()) {
            synchronized (requestQueue) {
                requestQueue.clear();
                addRequestStop();
            }

            synchronized (dataQueue) {
                disposeDataList(dataQueue);
                dataQueue.clear();
                addDataStop();
            }
        }
    }

    /**
     * Class which does all the hard work.
     */
    class WorkerThread
        implements Runnable
    {
        private static final long LOOP_FREQUENCY = Long.MAX_VALUE;

        /** Actual thread object (needed for start() method) */
        private Thread thread;
        /** <tt>true</tt> if there are no more requests. */
        private boolean reqStopped;
        /** <tt>true</tt> if there are no more data payloads. */
        private boolean dataStopped;

        private long numLoops;
        private long prevTrigF;
        private int prevTrigQ;
        private long prevDataF;
        private int prevDataQ;

        private String lastOp;
        private String baseMsg;

        /**
         * Create and start worker thread.
         *
         * @param name thread name
         */
        WorkerThread(String name)
        {
            thread = new Thread(this);
            thread.setName(name);
        }

        /**
         * Drop any unused requests.
         */
        private void clearCache()
        {
            synchronized (requestQueue) {
                final int numLeft = requestQueue.size();

                if (numLeft > 0 && LOG.isErrorEnabled()) {
                    LOG.error("clearCache() called for " + numLeft +
                              " requests in " + threadName);
                }

                boolean sawStop = false;
                for (int i = 0; i < numLeft; i++) {
                    ILoadablePayload data =
                        (ILoadablePayload) requestQueue.get(i);
                    if (data == STOP_MARKER) {
                        if (sawStop && LOG.isErrorEnabled()) {
                            LOG.error("Saw multiple request stops in " +
                                      threadName);
                        }

                        sawStop = true;
                        totRequestStops++;
                    } else if (data == null) {
                        if (LOG.isErrorEnabled()) {
                            LOG.error("Dropping null request#" + (i + 1) +
                                      " in " + threadName);
                        }
                    } else {
                        numDroppedRequests++;
                        data.recycle();
                    }
                }

                if (numLeft > 0 && !sawStop && LOG.isErrorEnabled()) {
                    LOG.error("Didn't see stop message while" +
                              " clearing request cache for " + threadName);
                }

                requestQueue.clear();
            }
        }

        /**
         * Get next payload from input queue.
         *
         * @return payload or <tt>null</tt>
         *         and set appropriate value in <tt>state</tt> attribute
         */
        ILoadablePayload getData()
        {
            ILoadablePayload data =
                (ILoadablePayload) syncRemove(dataQueue, true);

            numDataFetched++;

            if (data == STOP_MARKER) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found Stop data in " + threadName);
                }

                if (dataStopped && LOG.isErrorEnabled()) {
                    LOG.error("Saw multiple splicer stops in " + threadName);
                }

                dataStopped = true;
                totDataStops++;

                data = null;

                state = State.STOP_DATA;
            } else if (data == null) {
                numNullData++;

                if (LOG.isErrorEnabled()) {
                    LOG.error("Saw null data payload in " + threadName);
                }

                state = State.ERR_NULL_DATA;
            } else if (reqStopped) {
                disposeData(data);

                numDroppedData++;
                data = null;

                state = State.TOSSED_DATA;
            } else {
                try {
                    data.loadPayload();
                } catch (Exception ex) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Couldn't load " + data.getClass().getName(),
                                  ex);
                    }

                    data = null;
                    numBadData++;
                    totBadData++;
                }

                if (data == null) {
                    state = State.ERR_BAD_DATA;
                } else {
                    state = State.LOADED_DATA;
                }
            }

            return data;
        }

        public String getBaseMsg()
        {
            final int numTrigQ = getNumRequestsQueued();
            final int numDataQ = getNumDataPayloadsQueued();

            if (prevTrigF != numReqFetched || prevTrigQ != numTrigQ ||
                prevDataF != numDataFetched || prevDataQ != numDataQ)
            {
                baseMsg =
                    String.format("l%d r%d/q%d d%d/q%d", numLoops,
                                  numReqFetched,
                                  getNumRequestsQueued(),
                                  numDataFetched,
                                  getNumDataPayloadsQueued());

                prevTrigF = numReqFetched;
                prevTrigQ = numTrigQ;
                prevDataF = numDataFetched;
                prevDataQ = numDataQ;
            }

            return baseMsg;
        }

        public String getDebugMsg()
        {
            return getBaseMsg() + " " + lastOp;
        }

        /**
         * Get next request from request queue.
         *
         * @return request or <tt>null</tt>
         *         and set appropriate value in <tt>state</tt> attribute
         */
        ILoadablePayload getRequest()
        {
            ILoadablePayload req =
                (ILoadablePayload) syncRemove(requestQueue, false);

            numReqFetched++;

            if (req == STOP_MARKER) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found Stop request in " + threadName);
                }

                if (reqStopped && LOG.isErrorEnabled()) {
                    LOG.error("Saw multiple request stops in " + threadName);
                }

                reqStopped = true;
                totRequestStops++;

                req = null;

                state = State.STOP_REQUEST;
            } else if (req == null) {
                numEmptyLoops++;

                state = State.EMPTY_LOOP;
            } else {
                try {
                    req.loadPayload();
                } catch (Exception ex) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Couldn't load request payload for " +
                                  threadName, ex);
                    }

                    req = null;
                }

                if (req == null) {
                    numBadRequests++;

                    state = State.ERR_BAD_REQUEST;
                } else {
                    setRequestTimes(req);

                    state = State.LOADED_REQUEST;
                }
            }

            return req;
        }

        /**
         * Main processing loop.
         */
        public void run()
        {
            try {
                runInternal();
            } catch (Throwable thr) {
                LOG.error("Main loop failed", thr);
            }
        }

        /**
         * Real processing loop.
         */
        private void runInternal()
        {
            LOG.error("Starting main loop");

            ILoadablePayload curData = null;
            ILoadablePayload curReq = null;

            boolean dangerZone = sendEmptyPayloads;
            long numChanges = 0;

            while (!reqStopped || !dataStopped || curData != null) {
                if (dangerZone) {
                    numLoops++;

                    if (numLoops % LOOP_FREQUENCY == 0) {
                        LOG.error(getBaseMsg());
                    }

                    if (numOutputsSent > 250000) {
                        dangerZone = false;
                        baseMsg = "[safe zone] ";
                    }
                }

                // get next request
                if (curReq == null && !reqStopped) {
                    lastOp = "getReq";
                    curReq = getRequest();
                    lastOp = "gotReq";

                    if (reqStopped && curData != null) {
                        disposeData(curData);
                        curData = null;
                    }
                }

                // get next data payload
                if (curData == null && dataQueue.size() > 0) {
                    lastOp = "getData";
                    curData = getData();
                    lastOp = "gotData";
                }

                // if we're out of requests but still have data
                if (curReq == null && reqStopped && curData != null) {
                    disposeData(curData);
                    curData = null;
                }

                // try to fit the data with the request
                if (curReq != null && (dataStopped || curData != null)) {
                    lastOp = "fillReq";
                    if (dataStopped && curData == null &&
                        requestedData.size() == 0)
                    {
                        // no more data, discard the current request

                        numDroppedRequests++;

                        curReq.recycle();
                        curReq = null;

                        state = State.TOSSED_REQUEST;
                    } else {
                        final int cmp;
                        if (curData == null) {
                            cmp = 1;
                        } else {
                            lastOp = "compare";
                            cmp = compareRequestAndData(curReq, curData);
                            lastOp = "compared";
                        }

                        if (cmp < 0) {
                            // data is before current request, throw it away

                            lastOp = "cmpLT";
                            disposeData(curData);
                            curData = null;

                            numDataDiscarded++;
                            totDataDiscarded++;

                            state = State.EARLY_DATA;
                        } else if (cmp == 0) {
                            // data is within the current request

                            lastOp = "cmpEQ";
                            if (!isRequested(curReq, curData)) {
                                numDataDiscarded++;
                                totDataDiscarded++;

                                disposeData(curData);

                                state = State.TOSSED_DATA;
                            } else {
                                requestedData.add(curData);

                                state = State.SAVED_DATA;
                            }

                            curData = null;
                        } else {
                            lastOp = "cmpGT";
                            if (requestedData.size() == 0 &&
                                !sendEmptyPayloads)
                            {
                                // ignore empty payloads
                                numOutputsIgnored++;
                                totOutputsIgnored++;

                                if (LOG.isDebugEnabled() &&
                                    numOutputsIgnored % 1000 == 0)
                                {
                                    LOG.debug("Ignoring empty output payload" +
                                              " #" + numOutputsIgnored +
                                              " in " + threadName);
                                }

                                state = State.OUTPUT_IGNORED;
                            } else {
                                // data is past current request, build output!

                                lastOp = "build";

                                // keep track of number of payloads used
                                numDataUsed += requestedData.size();

                                // build the output payload
                                ILoadablePayload payload =
                                    makeDataPayload(curReq, requestedData);

                                if (payload == null) {
                                    // couldn't create output payload

                                    if (LOG.isErrorEnabled()) {
                                        LOG.error("Could not create output " +
                                                  "payload in " + threadName);
                                    }

                                    numNullOutputs++;

                                    state = State.ERR_NULL_OUTPUT;
                                } else if (payload != DROPPED_PAYLOAD) {
                                    // send the output payload

                                    lastOp = "send";

                                    final long payTime = payload.getUTCTime();
                                    if (payTime >= 0 && sendOutput(payload)) {
                                        synchronized (outputDataLock) {
                                            lastOutputTime = payTime;
                                            if (firstOutputTime == 0) {
                                                firstOutputTime = payTime;
                                            }
                                            numOutputsSent++;
                                            totOutputsSent++;
                                        }

                                        state = State.OUTPUT_SENT;
                                    } else {
                                        if (payTime < 0) {
                                            if (LOG.isErrorEnabled()) {
                                                final String msg =
                                                    "Could not send payload" +
                                                    " with negative time: " +
                                                    payload;
                                                LOG.error(msg);
                                            }
                                        }

                                        numOutputsFailed++;
                                        totOutputsFailed++;

                                        state = State.OUTPUT_FAILED;
                                    }
                                }
                            }

                            lastOp = "clean";

                            // clean up request memory
                            curReq.recycle();
                            curReq = null;

                            // clean up data memory
                            if (requestedData.size() > 0) {
                                disposeDataList(requestedData);
                                requestedData.clear();
                            }

                            // if there are too many failures, abort
                            if (numOutputsFailed > maxOutputFailures) {
                                if (curData != null) {
                                    disposeData(curData);
                                    curData = null;
                                }

                                reqStopped = true;
                                dataStopped = true;
                            }
                        }
                    }
                }
            }
            LOG.error("reqStopped " + reqStopped + " dataStopped " +
                      dataStopped + " curData " + curData);

            // clean up before exiting
            clearCache();
            recycleFinalData();
            finishThreadCleanup();

            workerThread = null;

            LOG.error("Exiting main loop");
        }

        /**
         * Start the thread.
         */
        void start()
        {
            thread.start();
        }

        /**
         * Remove the first object from the list in a thread-safe manner.
         *
         * @param list list of objects
         * @param isData <tt>true</tt> if we're removing a data payload,
         *               otherwise it must be a request payload
         *
         * @return removed object
         */
        private Object syncRemove(List list, boolean isData)
        {
            if (list == null) {
                return null;
            }

            Object obj;

            synchronized (list) {
                if (list.size() == 0) {
                    state = (isData ? State.WAIT_DATA : State.WAIT_REQUEST);

                    try {
                        list.wait(100);
                    } catch (InterruptedException ie) {
                        String objName;
                        if (isData) {
                            objName = "data";
                        } else {
                            objName = "request";
                        }

                        LOG.error("Couldn't wait for " + objName + " in " +
                                  threadName, ie);
                    }
                }

                if (list.size() == 0) {
                    obj = null;
                } else {
                    obj = list.remove(0);
                }
            }

            state = (isData ? State.GOT_DATA : State.GOT_REQUEST);

            return obj;
        }

        public String toString()
        {
            return threadName;
        }
    }
}
