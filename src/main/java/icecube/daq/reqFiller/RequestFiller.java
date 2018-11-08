package icecube.daq.reqFiller;

import icecube.daq.io.StreamMetaData;
import icecube.daq.payload.ILoadablePayload;
import icecube.daq.payload.IPayload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

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
    private static final Logger LOG = Logger.getLogger(RequestFiller.class);

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
    private long firstOutputTime;
    private long lastOutputTime;
    private long numDataReceived;
    private long numOutputsFailed;
    private long numOutputsIgnored;
    private long numOutputsSent;
    private long numRequestsReceived;

    // lifetime monitoring counters
    private long totDataReceived;
    private long totDataStops;
    private long totRequestStops;

    // ceiling for number of failed outputs
    private long maxOutputFailures = 10;

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
        if (!isRunning()) {
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
     * Get first output time.
     *
     * @return first output time
     */
    public long getFirstOutputTime()
    {
        return firstOutputTime;
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
     * Get the number of dispatched events and last dispatched time.
     *
     * @return metadata object
     */
    public StreamMetaData getMetaData()
    {
        return new StreamMetaData(numOutputsSent, lastOutputTime);
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
     * Get number of outputs sent.
     *
     * @return number of outputs sent
     */
    public long getNumOutputsSent()
    {
        return numOutputsSent;
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
     * Total number of stop messages received from the request source.
     *
     * @return total number of received stop messages
     */
    public long getTotalRequestStopsReceived()
    {
        return totRequestStops;
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
     * Reset the request filler after it has been stopped.
     */
    public void reset()
    {
        resetCounters();

        if (dataQueue.size() > 0) {
            LOG.error("Data payloads queued at " + threadName + " reset");

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
        firstOutputTime = 0;
        lastOutputTime = 0;
        numDataReceived = 0;
        numOutputsFailed = 0;
        numOutputsSent = 0;
        numRequestsReceived = 0;
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
            LOG.error("Thread " + threadName + " already running!");
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
        /** Number of sequential NPEs allowed before the thread is killed */
        private static final int MAX_NULL_POINTER_EXCEPTIONS = 10;

        /** Actual thread object (needed for start() method) */
        private Thread thread;
        /** <tt>true</tt> if there are no more requests. */
        private boolean reqStopped;
        /** <tt>true</tt> if there are no more data payloads. */
        private boolean dataStopped;

        /** NullPointerException counter */
        private int nullPtrCount;

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

                if (numLeft > 0) {
                    LOG.error("clearCache() called for " + numLeft +
                              " requests in " + threadName);
                }

                boolean sawStop = false;
                for (int i = 0; i < numLeft; i++) {
                    ILoadablePayload data =
                        (ILoadablePayload) requestQueue.get(i);
                    if (data == STOP_MARKER) {
                        if (sawStop) {
                            LOG.error("Saw multiple request stops in " +
                                      threadName);
                        }

                        sawStop = true;
                        totRequestStops++;
                    } else if (data == null) {
                        LOG.error("Dropping null request#" + (i + 1) +
                                  " in " + threadName);
                    } else {
                        data.recycle();
                    }
                }

                if (numLeft > 0 && !sawStop) {
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
         *
         * @throws IOException if we repeatedly hit the mysterious null pointer
         *         exception
         */
        private ILoadablePayload getData()
            throws IOException
        {
            ILoadablePayload data;
            try {
                data = (ILoadablePayload) syncRemove(dataQueue, true);
                nullPtrCount = 0;
            } catch (NullPointerException npe) {
                if (nullPtrCount++ < MAX_NULL_POINTER_EXCEPTIONS) {
                    LOG.error("Ignoring NPE#" + nullPtrCount, npe);
                    data = null;
                } else {
                    throw new IOException("Cannot get data after " +
                                          nullPtrCount + " attempts", npe);
                }
            }

            if (data == STOP_MARKER) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found Stop data in " + threadName);
                }

                if (dataStopped) {
                    LOG.error("Saw multiple splicer stops in " + threadName);
                }

                dataStopped = true;
                totDataStops++;

                data = null;
            } else if (data == null) {
                LOG.error("Saw null data payload in " + threadName);
            } else if (reqStopped) {
                disposeData(data);

                data = null;
            } else {
                try {
                    data.loadPayload();
                } catch (Exception ex) {
                    LOG.error("Couldn't load " + data.getClass().getName(),
                              ex);

                    data = null;
                }
            }

            return data;
        }

        /**
         * Get next request from request queue.
         *
         * @return request or <tt>null</tt>
         */
        ILoadablePayload getRequest()
        {
            ILoadablePayload req =
                (ILoadablePayload) syncRemove(requestQueue, false);

            if (req == STOP_MARKER) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found Stop request in " + threadName);
                }

                if (reqStopped) {
                    LOG.error("Saw multiple request stops in " + threadName);
                }

                reqStopped = true;
                totRequestStops++;

                req = null;
            } else if (req != null) {
                try {
                    req.loadPayload();
                } catch (Exception ex) {
                    LOG.error("Couldn't load request payload for " +
                              threadName, ex);

                    req = null;
                }

                if (req != null) {
                    setRequestTimes(req);
                }
            }

            return req;
        }

        /**
         * Main processing loop.
         */
        @Override
        public void run()
        {
            ILoadablePayload curData = null;
            ILoadablePayload curReq = null;

            while (!reqStopped || !dataStopped || curData != null) {
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
                    try {
                        curData = getData();
                    } catch (IOException ioe) {
                        LOG.error("Stopping thread due to unexpected" +
                                  " exception", ioe);
                        // XXX this may still cause trouble, but I'd like to
                        // at least *try* to shut down as nicely as possible
                        try {
                            stopThread();
                        } catch (IOException ioe2) {
                            LOG.error("Failed to stop thread; aborting", ioe2);
                            break;
                        }
                    }
                }

                // if we're out of requests but still have data
                if (curReq == null && reqStopped && curData != null) {
                    disposeData(curData);
                    curData = null;
                }

                if (curReq == null || (!dataStopped && curData == null)) {
                    // if there's no data, give other threads a chance
                    Thread.yield();
                } else {
                    // try to fit the data with the request
                    if (dataStopped && curData == null &&
                        requestedData.size() == 0)
                    {
                        // no more data, discard the current request
                        curReq.recycle();
                        curReq = null;
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
                        } else if (cmp == 0) {
                            // data is within the current request

                            if (!isRequested(curReq, curData)) {
                                disposeData(curData);
                            } else {
                                requestedData.add(curData);
                            }

                            curData = null;
                        } else {
                            if (requestedData.size() == 0 &&
                                !sendEmptyPayloads)
                            {
                                // ignore empty payloads
                                numOutputsIgnored++;

                                if (LOG.isDebugEnabled() &&
                                    numOutputsIgnored % 1000 == 0)
                                {
                                    LOG.debug("Ignoring empty output payload" +
                                              " #" + numOutputsIgnored +
                                              " in " + threadName);
                                }
                            } else {
                                // data is past current request, build output!

                                // build the output payload
                                ILoadablePayload payload =
                                    makeDataPayload(curReq, requestedData);

                                if (payload == null) {
                                    // couldn't create output payload

                                    LOG.error("Could not create output " +
                                              "payload in " + threadName);
                                } else if (payload != DROPPED_PAYLOAD) {
                                    // send the output payload

                                    final long payTime = payload.getUTCTime();
                                    if (payTime >= 0 && sendOutput(payload)) {
                                        synchronized (outputDataLock) {
                                            lastOutputTime = payTime;
                                            if (firstOutputTime == 0) {
                                                firstOutputTime = payTime;
                                            }
                                            numOutputsSent++;
                                        }
                                    } else {
                                        if (payTime < 0) {
                                            final String msg =
                                                "Could not send payload" +
                                                " with negative time: " +
                                                payload;
                                            LOG.error(msg);
                                        }

                                        numOutputsFailed++;
                                    }
                                }
                            }

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

            // clean up before exiting
            clearCache();
            finishThreadCleanup();

            workerThread = null;
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
                    try {
                        obj = list.remove(0);
                    } catch (NullPointerException npe) {
                        // guard against a one-in-a-quadrillion bug
                        obj = null;
                    }
                }
            }

            return obj;
        }

        @Override
        public String toString()
        {
            return threadName;
        }
    }
}
