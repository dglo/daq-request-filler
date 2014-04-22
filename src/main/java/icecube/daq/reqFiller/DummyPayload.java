package icecube.daq.reqFiller;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.ILoadablePayload;
import icecube.daq.payload.IUTCTime;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DummyPayload
    implements ILoadablePayload
{
    public DummyPayload() {
    }

    /**
     * This method allows a deepCopy of itself.
     *
     * @return Object which is a copy of the object which implements this interface.
     */
    public Object deepCopy() {
        return new DummyPayload();
    }

    public ByteBuffer getPayloadBacking()
    {
        throw new Error("Unimplemented");
    }

    /**
     * returns the Payload interface type as defined in the PayloadInterfaceRegistry.
     *
     * @return int ... one of the defined types in icecube.daq.payload.PayloadInterfaceRegistry
     */
    public int getPayloadInterfaceType() {
        return -1;
    }

    /**
     * returns the length in bytes of this payload
     */
    public int getPayloadLength() {
        return length();
    }

    /**
     * gets the UTC time tag of a payload
     */
    public IUTCTime getPayloadTimeUTC() {
        return null;
    }

    /**
     * returns the Payload type
     */
    public int getPayloadType() {
        return -1;
    }

    /**
     * gets the UTC time tag of a payload as a long value
     */
    public long getUTCTime()
    {
        return 0;
    }

    public int length()
    {
        return 0;
    }

    public void loadPayload()
    {
        // do nothing
    }

    public void recycle()
    {
        // do nothing
    }

    public void setCache(IByteBufferCache cache)
    {
        // do nothing
    }

    public String toString()
    {
        return "Dummy";
    }
}
