package icecube.daq.reqFiller;

import icecube.daq.payload.IPayload;

import icecube.daq.payload.splicer.Payload;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Generic request fulfillment engine.
 */
public abstract class RequestFiller
{
    /** Stop marker for request and data queues. */
    private static final StopMarker STOP_MARKER = StopMarker.INSTANCE;

    /** Message logger. */
    private static final Log LOG = LogFactory.getLog(RequestFiller.class);

    /** Unknown back-end state. */
    private static final int STATE_ERR_UNKNOWN = 0;
    /** Generator was called with null data. */
    private static final int STATE_ERR_NULL_DATA = 2;
    /** Could not create an output payload. */
    private static final int STATE_ERR_NULL_OUTPUT = 3;
    /** Back-end could not load the request. */
    private static final int STATE_ERR_BAD_REQUEST = 4;
    /** Back-end could not load the data payload. */
    private static final int STATE_ERR_BAD_DATA = 5;

    /** A stop marker was pulled from the request queue. */
    private static final int STATE_STOP_REQUEST = 6;
    /** A stop marker was pulled from the data queue. */
    private static final int STATE_STOP_DATA = 7;

    /** No request available on this pass through main loop. */
    private static final int STATE_EMPTY_LOOP = 8;
    /** Back-end is waiting for a request or data. */
    private static final int STATE_WAITING = 9;
    /** Back-end got a request from the queue. */
    private static final int STATE_GOT_REQUEST = 10;
    /** Back-end got data from the queue. */
    private static final int STATE_GOT_DATA = 11;
    /** Back-end successfully loaded the request. */
    private static final int STATE_LOADED_REQUEST = 12;
    /** Back-end successfully loaded the data payload. */
    private static final int STATE_LOADED_DATA = 13;
    /** Back-end could not use the request, so it was thrown out. */
    private static final int STATE_TOSSED_REQUEST = 14;
    /** Back-end could not use the data, so it was thrown out. */
    private static final int STATE_TOSSED_DATA = 15;
    /** Data was before current request. */
    private static final int STATE_EARLY_DATA = 16;
    /** Data will be sent. */
    private static final int STATE_SAVED_DATA = 17;
    /** Back-end sent output payload. */
    private static final int STATE_OUTPUT_SENT = 18;
    /** Back-end failed to send output payload. */
    private static final int STATE_OUTPUT_FAILED = 19;
    /** Back-end ignored empty output payload. */
    private static final int STATE_OUTPUT_IGNORED = 20;

    /** Total number of states. */
    private static final int NUM_STATES = 21;

    /** Names corresponding to the described states. */
    private static final String[] STATE_NAMES = new String[] {
        "Unknown state",
        "Null data payload",
        "Null hit",
        "Null request",
        "Null output",

        "Global request STOP received",
        "Splicer STOP received",

        "Waiting for input",
        "Got request",
        "Got data",
        "Loaded request",
        "Loaded data",
        "Bad request",
        "Bad data",
        "Threw away unused request",
        "Threw away unused data",
        "Got early data",
        "Data was saved",
        "Sent output",
        "Could not send output",
        "Ignored empty output",
    };

    /** Back-end thread name. */
    private String threadName;
    /** <tt>true</tt> if empty output payloads should be sent. */
    private boolean sendEmptyPayloads;

    /** Request queue -- ACCESS MUST BE SYNCHRONIZED. */
    private List requestQueue = new ArrayList();
    /** Data queue -- ACCESS MUST BE SYNCHRONIZED. */
    private List dataQueue = new ArrayList();

    /** accumulator for data to be sent in next output payload. */
    private List requestedData = new ArrayList();

    // per-run monitoring counters
    private long dataPerSecX100;
    private long numBadData;
    private long numBadRequests;
    private long numDataDiscarded;
    private long numDataReceived;
    private long numDataUsed;
    private long numDroppedRequests;
    private long numEmptyLoops;
    private long numNullData;
    private long numNullOutputs;
    private long numOutputsFailed;
    private long numOutputsIgnored;
    private long numOutputsSent;
    private long numRequestsReceived;
    private long numUnusedData;
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

    // current back-end state
    private int state = STATE_ERR_UNKNOWN;

    private BackEndThread thread;

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
     */
    public void addData(List newData, int offset)
    {
        if (!isRunning() && LOG.isErrorEnabled()) {
            LOG.error("Adding list of data while thread " + threadName +
                      " is stopped");
        }

        synchronized (dataQueue) {
            // adjust offset to fit within legal bounds
            if (offset < 0) {
                offset = 0;
            } else if (offset > newData.size()) {
                offset = newData.size();
            }

            final int newLen = newData.size() - offset;
            final int oldLen = dataQueue.size();

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
     */
    public void addData(IPayload newData)
    {
        if (!isRunning() && LOG.isErrorEnabled()) {
            LOG.error("Adding data while thread " + threadName +
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
     */
    public void addDataStop()
    {
        if (!isRunning()) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Adding data stop while thread " + threadName +
                          " is stopped");
            }

            return;
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
     */
    public void addRequest(IPayload newReq)
    {
        if (!isRunning() && LOG.isErrorEnabled()) {
            LOG.error("Adding request while thread " + threadName +
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
     */
    public void addRequestStop()
    {
        if (!isRunning()) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Adding request stop while thread " + threadName +
                          " is stopped");
            }

            return;
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
    public abstract void disposeData(IPayload data);

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
     * Get current back-end state.
     *
     * @return state string
     */
    public String getBackEndState()
    {
        if (STATE_NAMES.length != NUM_STATES) {
            throw new Error("Expected " + NUM_STATES + " state names, not " +
                            STATE_NAMES.length);
        } else if (state < 0 || state >= NUM_STATES) {
            throw new Error("Illegal state #" + state);
        }

        return STATE_NAMES[state];
    }

    /**
     * Get back-end timing profile.
     *
     * @return back-end timing profile
     */
    public String getBackEndTiming()
    {
        return (thread == null ? "NOT RUNNING" : thread.getTimerString());
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
     * Get number of data payloads not used for an event.
     *
     * @return number of unused data payloads
     */
    public long getNumUnusedDataPayloads()
    {
        return numUnusedData;
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
        return (thread != null);
    }

    /**
     * Make a payload out of the request in req and the data in dataList.
     *
     * @param reqPayload request
     * @param dataList list of data payloads matching the request
     *
     * @return The payload created for the current request.
     */
    public abstract IPayload makeDataPayload(IPayload reqPayload,
                                             List dataList);

    /**
     * Recycle payloads left after the final output payload.
     */
    public abstract void recycleFinalData();

    /**
     * Reset the back end after it has been stopped.
     */
    public void reset()
    {
        dataPerSecX100 = 0;
        numBadData = 0;
        numBadRequests = 0;
        numDataDiscarded = 0;
        numDataReceived = 0;
        numDataUsed = 0;
        numDroppedRequests = 0;
        numEmptyLoops = 0;
        numNullData = 0;
        numNullOutputs = 0;
        numOutputsFailed = 0;
        numOutputsSent = 0;
        numRequestsReceived = 0;
        numUnusedData = 0;
        outputPerSecX100 = 0;
        reqsPerSecX100 = 0;

        state = STATE_ERR_UNKNOWN;

        if (dataQueue.size() > 0) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Data payloads queued at " + threadName + " reset");
            }

            disposeDataList(dataQueue);
            dataQueue.clear();
        }

        if (!isRunning()) {
            startThread();
        }
    }

    /**
     * Send the output payload.
     *
     * @param payload payload to send
     *
     * @return <tt>false</tt> if payload could not be sent
     */
    public abstract boolean sendOutput(IPayload payload);

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
            thread = new BackEndThread(threadName);
        }
    }

    /**
     * If the thread is running, stop it.
     */
    public void stopThread()
    {
        if (isRunning()) {
            synchronized (requestQueue) {
                requestQueue.clear();
                addRequestStop();
            }

            synchronized (dataQueue) {
                dataQueue.clear();
                addDataStop();
            }
        }
    }

    /**
     * Class which does all the hard work.
     */
    class BackEndThread
        implements Runnable
    {
        /** <tt>true</tt> if there are no more requests. */
        private boolean reqStopped;
        /** <tt>true</tt> if there are no more data payloads. */
        private boolean dataStopped;

        /** half-assed profiling data */
        private BackEndTimer timer = new BackEndTimer();

        /**
         * Create and start back-end thread.
         *
         * @param name thread name
         */
        BackEndThread(String name)
        {
            Thread tmpThread = new Thread(this);
            tmpThread.setName(name);
            tmpThread.start();
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
                    IPayload data = (IPayload) requestQueue.get(i);
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
                        ((Payload) data).recycle();
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
         * Get next payload from splicer->back-end queue.
         *
         * @return payload or <tt>null</tt>
         *         and set appropriate value in <tt>state</tt> attribute
         */
        IPayload getData()
        {
            timer.start();

            IPayload data = (IPayload) syncRemove(dataQueue, true);

            timer.stop(BackEndTimer.GOT_DATA);

            timer.start();

            int timerId;
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

                state = STATE_STOP_DATA;
                timerId = BackEndTimer.STOP_DATA;
            } else if (data == null) {
                numNullData++;

                state = STATE_ERR_NULL_DATA;
                timerId = BackEndTimer.NULL_DATA;
            } else if (reqStopped) {
                disposeData(data);

                numUnusedData++;
                data = null;

                state = STATE_TOSSED_DATA;
                timerId = BackEndTimer.TOSS_DATA;
            } else {
                try {
                    ((Payload) data).loadPayload();
                } catch (Exception ex) {
                    LOG.error("Couldn't load " + data.getClass().getName(), ex);
                    data = null;
                    numBadData++;
                    totBadData++;
                }

                if (data == null) {
                    state = STATE_ERR_BAD_DATA;
                    timerId = BackEndTimer.BAD_DATA;
                } else {
                    state = STATE_LOADED_DATA;
                    timerId = BackEndTimer.LOAD_DATA;
                }
            }

            timer.stop(timerId);

            return data;
        }

        /**
         * Get string description of half-assed profiling data.
         *
         * @return profiling data
         */
        String getTimerString()
        {
            return timer.toString();
        }

        /**
         * Get next request from front-end->back-end queue.
         *
         * @return request or <tt>null</tt>
         *         and set appropriate value in <tt>state</tt> attribute
         */
        IPayload getRequest()
        {
            timer.start();

            IPayload req = (IPayload) syncRemove(requestQueue, false);

            timer.stop(BackEndTimer.GOT_RQST);

            timer.start();

            int timerId;
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

                state = STATE_STOP_REQUEST;
                timerId = BackEndTimer.STOP_RQST;
            } else if (req == null) {
                numEmptyLoops++;

                state = STATE_EMPTY_LOOP;
                timerId = BackEndTimer.EMPTY_LOOP;
            } else {
                try {
                    // TODO: There should be an interface for loadable payloads
                    ((Payload) req).loadPayload();
                } catch (Exception ex) {
                    LOG.error("Couldn't load request payload for " +
                              threadName, ex);
                    req = null;
                }

                if (req == null) {
                    numBadRequests++;

                    state = STATE_ERR_BAD_REQUEST;
                    timerId = BackEndTimer.BAD_RQST;
                } else {
                    setRequestTimes(req);

                    state = STATE_LOADED_REQUEST;
                    timerId = BackEndTimer.LOAD_RQST;
                }
            }

            timer.stop(timerId);

            return req;
        }

        /**
         * Main back-end processing loop.
         */
        public void run()
        {
            IPayload curData = null;
            IPayload curReq = null;

            // check the I/O rate every second
            final long rateInterval = 1000;

            long prevTime = System.currentTimeMillis();
            long rate = 0;
            long prevRcvd = 0;
            long prevReqs = 0;
            long prevSent = 0;

            while (!reqStopped || !dataStopped || curData != null) {
                timer.start();

                // monitor data I/O rates
                if (prevRcvd + rate < numDataReceived) {
                    final long curRcvd = numDataReceived;
                    final long curReqs = numRequestsReceived;
                    final long curSent = numOutputsSent;
                    final long curTime = System.currentTimeMillis();

                    if (prevTime + rateInterval < curTime) {
                        final long timeDiff = curTime - prevTime;

                        dataPerSecX100 =
                            ((curRcvd - prevRcvd) * rateInterval * 100) /
                            timeDiff;
                        reqsPerSecX100 =
                            ((curReqs - prevReqs) * rateInterval * 100) /
                            timeDiff;
                        outputPerSecX100 =
                            ((curSent - prevSent) * rateInterval * 100) /
                            timeDiff;

                        rate = dataPerSecX100 / 100;

                        prevTime = curTime;
                        prevRcvd = curRcvd;
                        prevReqs = curReqs;
                        prevSent = curSent;
                    }

                    timer.stop(BackEndTimer.RATE_MON);
                    timer.start();
                }

                // get next request
                if (curReq == null && !reqStopped) {
                    curReq = getRequest();

                    if (reqStopped && curData != null) {
                        disposeData(curData);
                        curData = null;
                    }
                }

                // get next data payload
                if (curData == null && dataQueue.size() > 0) {
                    curData = getData();
                }

                // if we're out of requests but still have data
                if (curReq == null && reqStopped && curData != null) {
                    disposeData(curData);
                    curData = null;
                }

                // try to fit the data with the request
                if (curReq != null && (dataStopped || curData != null)) {
                    timer.start();

                    int timerId;
                    if (dataStopped && curData == null &&
                        requestedData.size() == 0)
                    {
                        // no more data, discard the current request

                        numDroppedRequests++;

                        ((Payload) curReq).recycle();
                        curReq = null;

                        state = STATE_TOSSED_REQUEST;
                        timerId = BackEndTimer.TOSS_RQST;
                    } else {
                        final int cmp;
                        if (curData == null) {
                            cmp = 1;
                        } else {
                            cmp = compareRequestAndData(curReq, curData);
                        }

                        if (cmp < 0) {
                            // data is before current request, throw it away

                            disposeData(curData);
                            curData = null;

                            numDataDiscarded++;
                            totDataDiscarded++;

                            state = STATE_EARLY_DATA;
                            timerId = BackEndTimer.EARLY_DATA;
                        } else if (cmp == 0) {
                            // data is within the current request

                            if (!isRequested(curReq, curData)) {
                                disposeData(curData);

                                numDataDiscarded++;
                                totDataDiscarded++;

                                state = STATE_TOSSED_DATA;
                                timerId = BackEndTimer.TOSS_DATA;
                            } else {
                                requestedData.add(curData);

                                state = STATE_SAVED_DATA;
                                timerId = BackEndTimer.SAVED_DATA;
                            }

                            curData = null;
                        } else {
                            if (requestedData.size() == 0 &&
                                !sendEmptyPayloads)
                            {
                                // ignore empty payloads
                                numOutputsIgnored++;
                                totOutputsIgnored++;

                                state = STATE_OUTPUT_IGNORED;
                                timerId = BackEndTimer.IGNORE_OUT;
                            } else {
                                // data is past current request, build output!

                                // keep track of number of payloads used
                                numDataUsed += requestedData.size();

                                // build the output payload
                                IPayload payload =
                                    makeDataPayload(curReq, requestedData);

                                if (payload == null) {
                                    // couldn't create output payload

                                    if (LOG.isErrorEnabled()) {
                                        LOG.error("Could not create output " +
                                                  "payload in " + threadName);
                                    }

                                    numNullOutputs++;

                                    state = STATE_ERR_NULL_OUTPUT;
                                    timerId = BackEndTimer.NULL_OUT;
                                } else {
                                    // send the output payload

                                    timer.stop(BackEndTimer.MADE_OUT);

                                    timer.start();
                                    if (sendOutput(payload)) {
                                        numOutputsSent++;
                                        totOutputsSent++;

                                        state = STATE_OUTPUT_SENT;
                                        timerId = BackEndTimer.SENT_OUT;
                                    } else {
                                        numOutputsFailed++;
                                        totOutputsFailed++;

                                        state = STATE_OUTPUT_FAILED;
                                        timerId = BackEndTimer.FAIL_OUT;
                                    }
                                }
                            }

                            // clean up request memory
                            ((Payload) curReq).recycle();
                            curReq = null;

                            // clean up data memory
                            if (requestedData.size() > 0) {
                                disposeDataList(requestedData);
                                requestedData.clear();
                            }

                            timer.stop(timerId);
                        }
                    }
                }
            }

            // clean up before exiting
            clearCache();
            recycleFinalData();
            finishThreadCleanup();

            thread = null;
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
                    state = STATE_WAITING;

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

            state = (isData ? STATE_GOT_DATA : STATE_GOT_REQUEST);

            return obj;
        }
    }
}
