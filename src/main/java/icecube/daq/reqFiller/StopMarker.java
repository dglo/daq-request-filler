package icecube.daq.reqFiller;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.IDOMID;
import icecube.daq.payload.IEventHitRecord;
import icecube.daq.payload.IHitData;
import icecube.daq.payload.IHitDataPayload;
import icecube.daq.payload.IHitDataRecord;
import icecube.daq.payload.IHitPayload;
import icecube.daq.payload.IReadoutDataPayload;
import icecube.daq.payload.IReadoutRequest;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.ITriggerRequestPayload;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.PayloadException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Internal marker pushed onto a payload queue
 * to indicate that a STOP has been received.
 */
final class StopMarker
    implements IHitDataPayload, IHitPayload, IReadoutDataPayload,
               IReadoutRequest, ITriggerRequestPayload
{
    /** Faked payload type. */
    public static final int PAYLOAD_TYPE = 99;

    /** Singleton instance of stop marker. */
    static final StopMarker INSTANCE = new StopMarker();

    /**
     * Create a STOP marker for back-end queues.
     */
    private StopMarker()
    {
    }

    /**
     * Should not be used' throws an Error.
     */
    public void addElement(int type, int srcId, long firstTime, long lastTime,
                           long domId)
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public Object deepCopy()
    {
        throw new Error("StopMarker");
    }

    /**
     * Get channel ID
     * @return channel ID
     */
    @Override
    public short getChannelID()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public IDOMID getDOMID()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public List getDataPayloads()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public int getEmbeddedLength()
    {
        throw new Error("StopMarker");
    }

    /**
     * Get the hit record representation of this hit.
     *
     * @param chanId the channel ID for this hit's DOM ID
     *
     * @return hit record
     *
     * @throws PayloadException if there is a problem
     */
    public IEventHitRecord getEventHitRecord(short chanId)
        throws PayloadException
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public IUTCTime getFirstTimeUTC()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public List<IHitData> getHitList()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public IHitDataRecord getHitRecord()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public IUTCTime getHitTimeUTC()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public double getIntegratedCharge()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public IUTCTime getLastTimeUTC()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public int getNumHits()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public ByteBuffer getPayloadBacking()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public IUTCTime getPayloadTimeUTC()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public int getPayloadType()
    {
        return PAYLOAD_TYPE;
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public List getPayloads()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public int getReadoutDataPayloadNumber()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public IReadoutRequest getReadoutRequest()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public List getReadoutRequestElements()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public ISourceID getSourceID()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public int getTriggerConfigID()
    {
        throw new Error("StopMarker");
    }

    /**
     * Get the trigger name for the trigger type.
     *
     * @return trigger name
     */
    @Override
    public String getTriggerName()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public int getTriggerType()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public int getUID()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public long getUTCTime()
    {
        throw new Error("StopMarker");
    }

    /**
     * Return<tt>true</tt> if this hit has a channel ID instead of
     * source and DOM IDs
     */
    @Override
    public boolean hasChannelID()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public boolean isLastPayloadOfGroup()
    {
        throw new Error("StopMarker");
    }

    /**
     * Stop markers are not merged trigger requests.
     *
     * @return false
     */
    @Override
    public boolean isMerged()
    {
        return false;
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public int length()
    {
        throw new Error("StopMarker");
    }

    /**
     * Initializes Payload from backing so it can be used as an IPayload.
     */
    @Override
    public void loadPayload()
    {
        // nothing to load
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    @Override
    public int putBody(ByteBuffer buf, int offset)
    {
        throw new Error("StopMarker");
    }

    /**
     * Object knows how to recycle itself
     */
    @Override
    public void recycle()
    {
        // nothing to recycle
    }

    /**
     * Should not be used; throws an Error.
     */
    @Override
    public void setCache(IByteBufferCache cache)
    {
        throw new Error("StopMarker");
    }

    /**
     * Set the source ID. Needed for backward compatiblility with the old
     * global request handler implementation.
     *
     * @param srcId new source ID
     */
    @Override
    public void setSourceID(ISourceID srcId)
    {
        throw new Error("Unimplemented");
    }

    /**
     * Set the universal ID for global requests which will become events.
     *
     * @param uid new UID
     */
    @Override
    public void setUID(int uid)
    {
        throw new Error("Unimplemented");
    }

    /**
     * Object cannot write to a ByteBuffer
     */
    @Override
    public int writePayload(ByteBuffer buf, int offset)
        throws PayloadException
    {
        throw new Error("StopMarker");
    }

    /**
     * Object cannot write to a ByteBuffer
     */
    @Override
    public int writePayload(boolean writeLoaded, int destOffset,
                            ByteBuffer buf)
        throws IOException
    {
        throw new Error("StopMarker");
    }
}
