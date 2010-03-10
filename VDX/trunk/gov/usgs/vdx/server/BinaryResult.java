package gov.usgs.vdx.server;

import gov.usgs.net.NetTools;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.BinaryDataSet;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class BinaryResult extends RequestResult
{
	protected BinaryDataSet data;
	
	private transient ByteBuffer compressedBytes;
	
	public BinaryResult(BinaryDataSet d)
	{
		super();
		data = d;
	}
	
	public BinaryDataSet getData()
	{
		return data;
	}
	
	public void prepare()
	{
		ByteBuffer buffer = data.toBinary();
		byte[] cb = Util.compress(buffer.array(), 1);
		compressedBytes = ByteBuffer.wrap(cb);
		set("bytes", Integer.toString(compressedBytes.limit()));
	}
	
	protected void writeBody(NetTools netTools, SocketChannel channel)
	{
		netTools.writeByteBuffer(compressedBytes, channel);
	}
}
