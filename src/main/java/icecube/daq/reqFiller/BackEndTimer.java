package icecube.daq.reqFiller;

import icecube.daq.util.CodeTimer;

/**
 * Rudimentary profiler for payload request fulfillment engine.
 */
public class BackEndTimer
    extends CodeTimer
{
    /** Amount of time spent getting the next request. */
    public static final int GOT_RQST = 0;
    /** Time to handle stop marker from request queue. */
    public static final int STOP_RQST = 1;
    /** Time to handle empty loop. */
    public static final int EMPTY_LOOP = 2;
    /** Amount of time spent recognizing a bad request. */
    public static final int BAD_RQST = 3;
    /** Amount of time spent loading request payload. */
    public static final int LOAD_RQST = 4;

    /** Amount of time spent getting the next data payload from the queue. */
    public static final int GOT_DATA = 5;
    /** Time to handle stop marker from data queue. */
    public static final int STOP_DATA = 6;
    /** Time to handle null data error condition. */
    public static final int NULL_DATA = 7;
    /** Amount of time needed to discard early data payload. */
    public static final int TOSS_DATA = 8;
    /** Amount of time spent recognizing a bad data payload. */
    public static final int BAD_DATA = 9;
    /** Amount of time spent loading data payload. */
    public static final int LOAD_DATA = 10;

    /** Amount of time spent recognizing an unused request. */
    public static final int TOSS_RQST = 11;
    /** Time to deal with data earlier than current request. */
    public static final int EARLY_DATA = 12;
    /** Amount of time spent saving data to pending data list. */
    public static final int SAVED_DATA = 13;
    /** Time to handle null output payload error condition. */
    public static final int NULL_OUT = 14;
    /** Amount of time spent building output payload. */
    public static final int MADE_OUT = 15;
    /** Amount of time spent sending output payload. */
    public static final int SENT_OUT = 16;
    /** Amount of time spent failing to send output payload. */
    public static final int FAIL_OUT = 17;
    /** Amount of time spent ignoring empty output payload. */
    public static final int IGNORE_OUT = 18;

    /** Amount of time spent recycling payloads. */
    public static final int RECYCLE = 19;
    /** Amount of time spent monitoring back-end rates. */
    public static final int RATE_MON = 20;

    /** Total number of timer events. */
    public static final int NUM_TIMES = 21;

    private static final String[] TIME_NAMES = new String[] {
        "got req",
        "stop req",
        "null req",
        "bad req",
        "load req",

        "got data",
        "stop data",
        "null data",
        "toss data",
        "bad data",
        "load data",

        "toss req",
        "early data",
        "saved data",
        "*null out",
        "made out",
        "sent out",
        "fail out",
        "ignore out",

        "recycled",
        "rate mon",
    };

    /**
     * Create rudimentary profiler.
     */
    public BackEndTimer()
    {
        super(NUM_TIMES);
    }

    /**
     * Get current timing info.
     *
     * @return timing info
     */
    public String getStats()
    {
        if (TIME_NAMES.length != NUM_TIMES) {
            throw new Error("Expected " + NUM_TIMES + " titles, not " +
                            TIME_NAMES.length);
        }

        return getStats(TIME_NAMES);
    }

    /**
     * Get current timing info.
     *
     * @return timing info
     */
    public String toString()
    {
        return getStats();
    }
}
