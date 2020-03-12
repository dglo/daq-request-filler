package icecube.daq.reqFiller;

import icecube.daq.payload.IByteBufferCache;
import icecube.daq.payload.ILoadablePayload;
import icecube.daq.payload.IUTCTime;

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
    @Override
    public Object deepCopy() {
        return new DummyPayload();
    }

    @Override
    public ByteBuffer getPayloadBacking()
    {
        throw new Error("Unimplemented");
    }

    /**
     * gets the UTC time tag of a payload
     */
    @Override
    public IUTCTime getPayloadTimeUTC() {
        return null;
    }

    /**
     * returns the Payload type
     */
    @Override
    public int getPayloadType() {
        return -1;
    }

    /**
     * gets the UTC time tag of a payload as a long value
     */
    @Override
    public long getUTCTime()
    {
        return 0;
    }

    @Override
    public int length()
    {
        return 0;
    }

    @Override
    public void loadPayload()
    {
        // do nothing
    }

    @Override
    public void recycle()
    {
        // do nothing
    }

    @Override
    public void setCache(IByteBufferCache cache)
    {
        // do nothing
    }

    @Override
    public String toString()
    {
        return "Dummy";
    }
}
