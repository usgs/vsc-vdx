package gov.usgs.vdx.data.rsam;

import gov.usgs.util.Util;
import gov.usgs.vdx.data.GenericDataMatrix;
import gov.usgs.vdx.data.hypo.HypocenterList.BinSize;
import hep.aida.IAxis;
import hep.aida.ref.FixedAxis;
import hep.aida.ref.Histogram1D;
import hep.aida.ref.VariableAxis;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

/**
 * A class that deals with precomputed RSAM data.  The data are stored in a 2-D matrix, 
 * the first column is the time (j2ksec), the second is the data.
 *
 * $Log: not supported by cvs2svn $
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
	 * Create an RSAMData from a byte buffer.  This first 4 bytes specify an
	 * integer number of rows followed by rows*16 bytes, 2 doubles: j2ksec and 
	 * RSAM.
	 * 
	 * @param bb the byte buffer
	 */

	public EWRSAMData(List<double[]> list, List<double[]> e)
	{
		super(list);

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

	public ByteBuffer toBinary()
	{
		int rows = data.rows() + events.rows();
		int cols = data.columns() + events.columns();
		ByteBuffer bb = ByteBuffer.allocate(6 + (rows * cols) * 8);
		bb.putInt(data.rows());
		bb.putInt(data.columns());
		for (int i = 0; i < data.rows(); i++)
		{
			for (int j = 0; j < data.columns(); j++)
				bb.putDouble(data.getQuick(i, j));
		}
		
		bb.putInt(events.rows());
		bb.putInt(events.columns());
		for (int i = 0; i < events.rows(); i++)
		{
			for (int j = 0; j < events.columns(); j++)
				bb.putDouble(events.getQuick(i, j));
		}
		return bb;
	}
		
	public void fromBinary(ByteBuffer bb)
	{
		int rows = bb.getInt();
		int cols = bb.getInt();
		data = DoubleFactory2D.dense.make(rows, cols);
		for (int i = 0; i < rows; i++)
		{
			for (int j = 0; j < cols; j++)
				data.setQuick(i, j, bb.getDouble());
		}		
		rows = bb.getInt();
		cols = bb.getInt();
		events = DoubleFactory2D.dense.make(rows, cols);
		for (int i = 0; i < rows; i++)
		{
			for (int j = 0; j < cols; j++)
				events.setQuick(i, j, bb.getDouble());
		}		
	}
	
	public DoubleMatrix2D getCumulativeCounts()
	{		
		return events;
	}
	
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