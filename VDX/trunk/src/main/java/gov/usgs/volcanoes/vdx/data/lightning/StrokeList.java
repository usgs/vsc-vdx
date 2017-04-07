package gov.usgs.volcanoes.vdx.data.lightning;

import gov.usgs.plot.data.BinaryDataSet;
import gov.usgs.proj.Projection;
import gov.usgs.util.Util;
import hep.aida.IAxis;
import hep.aida.ref.FixedAxis;
import hep.aida.ref.Histogram1D;
import hep.aida.ref.VariableAxis;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

/**
 * Represent list of lightning strikes.
 * 
 * Modeled after Hypo.HypocenterList
 * 
 * @author Tom Parker
 */
public class StrokeList implements BinaryDataSet
{
	private static final int MAX_BINS = 1000000;
	public enum BinSize 
	{
		MINUTE("Minute"), TENMINUTE("TenMinute"), HOUR("Hour"), DAY("Day"), WEEK("Week"), MONTH("Month"), YEAR("Year");
		
		private String string;
		
		private BinSize(String s)
		{
			string = s;
		}
		
		public String toString()
		{
			return string;	
		}
		
		public static BinSize fromString(String s)
		{
			if (s == null)
				return null;
			switch(s.charAt(0))
			{
				case 'I':
					return MINUTE;
				case 'a':
					return TENMINUTE;
				case 'H':
					return HOUR;
				case 'D':
					return DAY;
				case 'W':
					return WEEK;
				case 'M':
					return MONTH;
				case 'Y':
					return YEAR;
				default:
					return null;
			}
		}
		
		public  int toSeconds()
		{
			switch(string.charAt(0))
			{
				case 'I':
					return 60;
				case 'a':
					return 600;
				case 'H':
					return 60*60;
				case 'D':
					return 60*60*24;
				case 'W':
					return 60*60*24*7;
				case 'M':
					return 60*60*24*7*4;
				case 'Y':
					return 60*60*24*7*52;
				default:
					return -1;
			}
		}

	}
	
	private List<Stroke> strokes;

	/**
	 * Default constructor
	 */
	public StrokeList()
	{
		this(new ArrayList<Stroke>(1));
	}
	
	/**
	 * Constructor.
	 * @param bb ByteBuffer to parse
	 * @see #fromBinary(ByteBuffer bb)
	 */
	public StrokeList(ByteBuffer bb)
	{
		fromBinary(bb);
	}
	
	/**
	 * Constructor
	 * @param ss List of strokes
	 */
	public StrokeList(List<Stroke> ss)
	{
		strokes = ss;
	}
	
	/**
	 * Get list of strokes
	 * @return list of strokes
	 */
	public List<Stroke> getStrokes()
	{
		return strokes;
	}
	
	/**
	 * Get list size
	 * @return number of strokes
	 */
	public int size()
	{
		return strokes.size();
	}
	
	/**
	 * Get time of first stroke in the list
	 * @return time of first event
	 */
	public double getStartTime()
	{
		if (strokes == null)
			return Double.NaN;
		else if (strokes.size() == 0)
			return 0;
		else
			return strokes.get(0).j2ksec;
	}
	
	/**
	 * Get time of last stroke in the list
	 * @return time of last event
	 */
	public double getEndTime()
	{
		if (strokes == null)
			return Double.NaN;
		else if (strokes.size() == 0)
			return 0;
		else
			return strokes.get(strokes.size() - 1).j2ksec;
	}
	
	/**
	 * Dump object content into ByteBuffer
	 * @return ByteBuffer of content
	 */
	public ByteBuffer toBinary()
	{
		ByteBuffer buffer = ByteBuffer.allocate(4 + strokes.size() * 6 * 8);
		buffer.putInt(strokes.size());
		for (Stroke stroke : strokes)
			stroke.insertIntoByteBuffer(buffer);
		buffer.flip();
		return buffer;
	}
	
	/**
	 * Parse ByteBuffer and fill list
	 * @see #toBinary()
	 */
	public void fromBinary(ByteBuffer bb)
	{
		int rows = bb.getInt();
		strokes = new ArrayList<Stroke>(rows);
		for (int i = 0; i < rows; i++) {
			strokes.add(new Stroke(bb.getDouble(), bb.getInt(), bb.getDouble(), bb.getDouble(), bb.getInt(), bb.getDouble()));
		}
		
	}
	
	
	
	/**
	 * Get string representation of stroke list
	 * @return string representation
	 */
	public String toString()
	{
		return "StrokeList: " + strokes.size() + " strokes";
	}
	
	/**
	 * Get initialized axis to use with histogram graph
	 * @param bin histogram section period
	 * @return initialized iaxis
	 */
	private IAxis getHistogramAxis(BinSize bin)
	{
		double startTime = getStartTime();
		double endTime = getEndTime();
		int bins = -1;
		IAxis axis = null;
		
		if (bin == BinSize.MINUTE)
		{
			startTime -= (startTime - 43200) % 60;
			endTime -= (endTime - 43200) % 60 - 60;
			bins = (int)(endTime - startTime) / 60;
			if (bins > MAX_BINS)
				bin = BinSize.HOUR;
			else
				axis = new FixedAxis(bins, startTime, endTime);
		}
		if (bin == BinSize.TENMINUTE)
		{
			startTime -= (startTime - 43200) % 600;
			endTime -= (endTime - 43200) % 600 - 600;
			bins = (int)(endTime - startTime) / 600;
			if (bins > MAX_BINS)
				bin = BinSize.HOUR;
			else
				axis = new FixedAxis(bins, startTime, endTime);
		}
		if (bin == BinSize.HOUR)
		{
			startTime -= (startTime - 43200) % 3600;
			endTime -= (endTime - 43200) % 3600 - 3600;
			bins = (int)(endTime - startTime) / 3600;
			if (bins > MAX_BINS)
				bin = BinSize.DAY;
			else
				axis = new FixedAxis(bins, startTime, endTime);
		}
		if (bin == BinSize.DAY)
		{
			startTime -= (startTime - 43200) % 86400;
			endTime -= (endTime - 43200) % 86400 - 86400;
			bins = (int)(endTime - startTime) / 86400;
			if (bins > MAX_BINS)
				bin = BinSize.WEEK;
			else
				axis = new FixedAxis(bins, startTime, endTime);
		}
		if (bin == BinSize.WEEK)
		{
			startTime -= (startTime - 43200) % 604800;
			endTime -= (endTime - 43200) % 604800 - 604800;
			bins = (int)(endTime - startTime) / 604800;
			if (bins > MAX_BINS)
				bin = BinSize.MONTH;
			else
				axis = new FixedAxis(bins, startTime, endTime);
		}
		if (bin == BinSize.MONTH)
		{
			Date ds = Util.j2KToDate(startTime);
			Date de = Util.j2KToDate(endTime);
			bins = Util.getMonthsBetween(ds, de) + 1;
			if (bins <= MAX_BINS)
			{
				Calendar cal = Calendar.getInstance();
				cal.setTime(ds);
				cal.set(Calendar.DAY_OF_MONTH, 1);
				cal.set(Calendar.HOUR_OF_DAY, 0);
				cal.set(Calendar.MINUTE, 0);
				cal.set(Calendar.SECOND, 0);
				cal.set(Calendar.MILLISECOND, 0);
				double[] edges = new double[bins + 1];
				for (int i = 0; i < bins + 1; i++)
				{
					edges[i] = Util.dateToJ2K(cal.getTime());
					cal.add(Calendar.MONTH, 1);
				}
				axis = new VariableAxis(edges);
			}
			else
				bin = BinSize.YEAR;
		}
		if (bin == BinSize.YEAR)
		{
			Date ds = Util.j2KToDate(startTime);  
			Date de = Util.j2KToDate(endTime);
			bins = Util.getYear(de) - Util.getYear(ds) + 1;
			double edges[] = new double[bins + 1];
			Calendar cal = Calendar.getInstance();
			cal.setTime(ds);
			cal.set(Calendar.MONTH, 1);
			cal.set(Calendar.DAY_OF_MONTH, 1);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			for (int i = 0; i < bins + 1; i++)
			{
				edges[i] = Util.dateToJ2K(cal.getTime());
				cal.add(Calendar.YEAR, 1);
			}
			axis = new VariableAxis(edges);
		}   
		return axis;
	}
	
	/**
	 * Compute 2-column matrix: time - cumulative event count
	 * @return matrix
	 */
	public DoubleMatrix2D getCumulativeCounts()
	{
		DoubleMatrix2D result = DoubleFactory2D.dense.make(strokes.size(), 2);
		for (int i = 0; i < strokes.size(); i++)
		{
			Stroke hc = strokes.get(i);
			result.setQuick(i, 0, hc.j2ksec);
			result.setQuick(i, 1, i);
		}
		return result;
	}
	
	/**
	 * Get initialized histogram of event count by time
	 * @param bin time interval
	 * @return histogram
	 */
	public Histogram1D getCountsHistogram(BinSize bin)
	{
		//if (hypocenters == null || hypocenters.size() == 0)
		if (strokes == null)
			return null;
		
		Histogram1D hist = new Histogram1D("", getHistogramAxis(bin));
		for (Stroke hc : strokes)
			hist.fill(hc.j2ksec);
		
		return hist;
	}
	
	/**
	 * Project of all hypocenters in the list
	 * @param proj Projection
	 */
	public void project(Projection proj)
	{
		for (Stroke hc : strokes)
			hc.project(proj);
	}
	
	/**
	 * Adds a value to the time (for time zone management).
	 * @param adj the time adjustment
	 */
	public void adjustTime(double adj)
	{
		for (Stroke hc : strokes)
			hc.j2ksec+=adj;
	}
	
	/**
	 * Dump list as CSV string
	 * @return string representation in CSV
	 */
	public String toCSV()
	{
		StringBuffer sb = new StringBuffer();
		for (Stroke hc : strokes)
			sb.append(hc.toString() + "\n");
		
		return sb.toString();
	}
}
