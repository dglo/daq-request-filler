package icecube.daq.reqFiller;

import icecube.daq.eventbuilder.IReadoutDataPayload;

import icecube.daq.payload.IDOMID;
import icecube.daq.payload.ILoadablePayload;
import icecube.daq.payload.ISourceID;
import icecube.daq.payload.IUTCTime;
import icecube.daq.payload.PayloadDestination;

import icecube.daq.trigger.IHitDataPayload;
import icecube.daq.trigger.IHitDataRecord;
import icecube.daq.trigger.IHitPayload;
import icecube.daq.trigger.IReadoutRequest;
import icecube.daq.trigger.ITriggerRequestPayload;

import java.io.IOException;

import java.nio.ByteBuffer;

import java.util.Vector;

import java.util.zip.DataFormatException;

/**
 * Internal marker pushed onto a payload queue
 * to indicate that a STOP has been received.
 */
final class StopMarker
    implements IHitDataPayload, IHitPayload, ILoadablePayload,
               IReadoutDataPayload, IReadoutRequest, ITriggerRequestPayload
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
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public Object deepCopy()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public void dispose()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public IDOMID getDOMID()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public Vector getDataPayloads()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public IUTCTime getFirstTimeUTC()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public Vector getHitList()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public IHitDataRecord getHitRecord()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public IUTCTime getHitTimeUTC()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public double getIntegratedCharge()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public IUTCTime getLastTimeUTC()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public ByteBuffer getPayloadBacking()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public int getPayloadLength()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public int getPayloadType()
    {
        return PAYLOAD_TYPE;
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public int getPayloadInterfaceType()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public IUTCTime getPayloadTimeUTC()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     *
     * @throws IOException never
     * @throws DataFormatException never
     */
    public Vector getPayloads()
        throws IOException, DataFormatException
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public int getReadoutDataPayloadNumber()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public IReadoutRequest getReadoutRequest()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public Vector getReadoutRequestElements()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public int getRequestUID()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public ISourceID getSourceID()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public int getTriggerConfigID()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public int getTriggerType()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public int getUID()
    {
        throw new Error("StopMarker");
    }

    /**
     * Should not be used; throws an Error.
     *
     * @return Error
     */
    public boolean isLastPayloadOfGroup()
    {
        throw new Error("StopMarker");
    }

    /**
     * Initializes Payload from backing so it can be used as an IPayload.
     */
    public void loadPayload()
    {
        // nothing to load
    }

    /**
     * Object knows how to recycle itself
     */
    public void recycle()
    {
        // nothing to recycle
    }

    /**
     * Object knows how to recycle itself
     */
    public int writePayload(boolean writeLoaded, PayloadDestination pDest)
        throws IOException
    {
        throw new Error("StopMarker");
    }

    /**
     * Object knows how to recycle itself
     */
    public int writePayload(boolean writeLoaded, int destOffset, ByteBuffer buf)
        throws IOException
    {
        throw new Error("StopMarker");
    }
}
