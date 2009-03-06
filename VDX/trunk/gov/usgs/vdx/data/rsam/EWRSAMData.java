package gov.usgs.vdx.data.rsam;

import gov.usgs.vdx.data.hypo.HypocenterList.BinSize;
import hep.aida.ref.Histogram1D;

import java.nio.ByteBuffer;
import java.util.List;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

/**
 * A class that deals with precomputed RSAM data.  The data are stored in a 2-D matrix, 
 * the first column is the time (j2ksec), the second is the data.
 *
 * @author Tom Parker
 */
public class EWRSAMData extends RSAMData
{
	
	protected static final int MAX_BINS = 1000000;
	public DoubleMatrix2D events;

	/** Generic empty constructor
	 */
	public EWRSAMData()
	{
		columnMap.put("time", 0);
		columnMap.put("rsam", 0);
	}

	/**
	 * Constructor.
	 * @param list rsam data
	 * @param e cumulative events data 
	 */
	public EWRSAMData(List<double[]> list, List<double[]> e)
	{
		super(list);

		if (e != null)
		{
			int rows = e.size();
			int cols = e.get(0).length;
			
			events = DoubleFactory2D.dense.make(rows, cols);
			for (int i = 0; i < rows; i++)
			{
				double[] d = (double[])e.get(i);
				for (int j = 0; j < cols; j++)
					events.setQuick(i, j, d[j]);
			}
			System.out.println("Events is " + events.size());
		}
	}

	/**
	 * Dump RSAMData to byte buffer. This first 4 bytes specify an
	 * integer number of rows followed by rows*16 bytes, 2 doubles: j2ksec and 
	 * RSAM.
	 */
	public ByteBuffer toBinary()
	{
		ByteBuffer dataBb;
		ByteBuffer eventsBb;
		ByteBuffer bb;
		
		if (data != null && data.size() != 0)
		{
			int rows = data.rows();
			int cols = data.columns();
			dataBb = ByteBuffer.allocate(8 + (rows * cols) * 8);
			
			dataBb.putInt(rows);
			dataBb.putInt(cols);
			for (int i = 0; i < rows; i++)
			{
				for (int j = 0; j < cols; j++)
					dataBb.putDouble(data.getQuick(i, j));
			}
		}
		else
		{
			dataBb = ByteBuffer.allocate(8);
			dataBb.putInt(0);
			dataBb.putInt(0);
		}

		if (events != null && events.size() != 0)
		{
			int rows = events.rows();
			int cols = events.columns();
			eventsBb = ByteBuffer.allocate(8 + (rows * cols) * 8);
			
			eventsBb.putInt(rows);
			eventsBb.putInt(cols);
			for (int i = 0; i < rows; i++)
			{
				for (int j = 0; j < cols; j++)
					eventsBb.putDouble(events.getQuick(i, j));
			}
		}
		else
		{
			eventsBb = ByteBuffer.allocate(8);
			eventsBb.putInt(0);
			eventsBb.putInt(0);
		}
		
		bb = ByteBuffer.allocate(dataBb.capacity() + eventsBb.capacity());
		bb.put(dataBb.array(), 0, dataBb.array().length);
		bb.put(eventsBb.array(), 0, eventsBb.array().length);

		return bb;
	}
	
	/**
	 * Initialize an RSAMData from a byte buffer.  
	 * @see toBinary()
	 * @param bb the byte buffer
	 */
	public void fromBinary(ByteBuffer bb)
	{
		int rows = bb.getInt();
		int cols = bb.getInt();
		if (rows > 0)
		{
			data = DoubleFactory2D.dense.make(rows, cols);
			for (int i = 0; i < rows; i++)
			{
				for (int j = 0; j < cols; j++)
					data.setQuick(i, j, bb.getDouble());
			}		
		}
		else
		{
			System.out.println(":: FOUND NO VALUES IN " + bb.capacity());
		}
		
		rows = bb.getInt();
		cols = bb.getInt();
		if (rows > 0)
		{
			events = DoubleFactory2D.dense.make(rows, cols);
			for (int i = 0; i < rows; i++)
			{
				for (int j = 0; j < cols; j++)
					events.setQuick(i, j, bb.getDouble());
			}		
		}
		else
		{
			System.out.println(":: FOUND NO EVENTS");
		}
	}
	
	/**
	 * Get cumulative event data by time interval
	 */
	public DoubleMatrix2D getCumulativeCounts()
	{		
		return events;
	}
	
	/**
	 * Get initialized histogram of event count by time
	 * @param bin time interval
	 */
	public Histogram1D getCountsHistogram(BinSize bin)
	{
		System.err.println("Entering ew hist");
		if (events == null || events.size() == 0)
		{
			System.out.println("No events");
			return null;
		}

		System.out.println("Filling histogram with " + events.size() + " points");

		Histogram1D hist = new Histogram1D("", getHistogramAxis(bin));
		
		for (int i=1; i<events.rows()-1; i++)
		{
			hist.fill(events.get(i,0));
		}
		
		return hist;
	}
}