package gov.usgs.vdx.data.hypo;

import gov.usgs.vdx.data.BinaryDataSet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class HypocenterList implements BinaryDataSet
{
	private List<Hypocenter> hypocenters;

	public HypocenterList()
	{}
	
	public HypocenterList(ByteBuffer bb)
	{
		fromBinary(bb);
	}
	
	public HypocenterList(List<Hypocenter> hs)
	{
		hypocenters = hs;
	}
	
	public List<Hypocenter> getHypocenters()
	{
		return hypocenters;
	}
	
	public void fromBinary(ByteBuffer bb)
	{
		int rows = bb.getInt();
		hypocenters = new ArrayList<Hypocenter>(rows);
		for (int i = 0; i < rows; i++)
		{
			double[] d = new double[5];
			for (int j = 0; j < 5; j++)
				d[j] = bb.getDouble();
			Hypocenter hc = new Hypocenter(d);
			hypocenters.add(hc);
		}
	}
	
	public ByteBuffer toBinary()
	{
		ByteBuffer buffer = ByteBuffer.allocate(4 + hypocenters.size() * 5 * 8);
		buffer.putInt(hypocenters.size());
		for (Hypocenter hc : hypocenters)
			hc.insertIntoByteBuffer(buffer);
		buffer.flip();
		return buffer;
	}
	
	public String toString()
	{
		return "HypocenterList: " + hypocenters.size() + " hypocenters";
	}
	
}
