package gov.usgs.volcanoes.vdx.server;

import gov.usgs.net.NetTools;
import gov.usgs.plot.data.BinaryDataSet;
import gov.usgs.util.Util;

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
	
	/**
	 * Constructor
	 * @param d binary data set
	 */
	public BinaryResult(BinaryDataSet d)
	{
		super();
		data = d;
	}
	
	/**
	 * Yield the data
	 * @return data
	 */
	public BinaryDataSet getData()
	{
		return data;
	}
	
	/**
	 * Get result ready for writing
	 */
	public void prepare()
	{
		ByteBuffer buffer = data.toBinary();
		byte[] cb = Util.compress(buffer.array(), 1);
		compressedBytes = ByteBuffer.wrap(cb);
		set("bytes", Integer.toString(compressedBytes.limit()));
	}
	
	/**
	 * Write data
	 * @param netTools tools to use for writing
	 * @param channel channel to write to
	 */
	protected void writeBody(NetTools netTools, SocketChannel channel)
	{
		netTools.writeByteBuffer(compressedBytes, channel);
	}
}
