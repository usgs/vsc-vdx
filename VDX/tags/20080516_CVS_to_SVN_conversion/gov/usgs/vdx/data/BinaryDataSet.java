package gov.usgs.vdx.data;

import java.nio.ByteBuffer;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public interface BinaryDataSet
{
	public ByteBuffer toBinary();
	public void fromBinary(ByteBuffer bb);
}
