package gov.usgs.vdx.data.hypo;

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
 * Represent list of hypocenters
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.6  2007/08/31 04:36:09  tparker
 * Add 10 minute bin size
 *
 * Revision 1.5  2006/06/09 00:48:56  tparker
 * Add toCSV for data export
 *
 * Revision 1.4  2005/12/23 00:52:13  tparker
 * avoid labeling issues described in bug id #86
 *
 * Revision 1.3  2005/10/07 17:03:16  dcervelli
 * Added size() method.
 *
 * Revision 1.2  2005/08/28 18:45:29  dcervelli
 * Added several methods for creating histograms.
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class HypocenterList implements BinaryDataSet
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
	
	private List<Hypocenter> hypocenters;

	/**
	 * Default constructor
	 */
	public HypocenterList()
	{
		this(new ArrayList<Hypocenter>(1));
	}
	
	/**
	 * Constructor.
	 * @param bb ByteBuffer to parse
	 * @see #fromBinary(ByteBuffer bb)
	 */
	public HypocenterList(ByteBuffer bb)
	{
		fromBinary(bb);
	}
	
	/**
	 * Constructor
	 * @param hs List of Hypocenters
	 */
	public HypocenterList(List<Hypocenter> hs)
	{
		hypocenters = hs;
	}
	
	/**
	 * Get list of hypocenters
	 * @return list of hypocenters
	 */
	public List<Hypocenter> getHypocenters()
	{
		return hypocenters;
	}
	
	/**
	 * Get list size
	 * @return number of hypocenters
	 */
	public int size()
	{
		return hypocenters.size();
	}
	
	/**
	 * Get time of first event in the list
	 * @return time of first event
	 */
	public double getStartTime()
	{
		if (hypocenters == null)
			return Double.NaN;
		else if (hypocenters.size() == 0)
			return 0;
		else
			return hypocenters.get(0).j2ksec;
	}
	
	/**
	 * Get time of last event in the list
	 * @return time of last event
	 */
	public double getEndTime()
	{
		if (hypocenters == null)
			return Double.NaN;
		else if (hypocenters.size() == 0)
			return 0;
		else
			return hypocenters.get(hypocenters.size() - 1).j2ksec;
	}
	
	/**
	 * Get shallowest depth in the list
	 * @return min depth in list
	 */
	public double getMinDepth(double md)
	{
		double minDepth = 0.0;
		if (md != Double.MIN_VALUE)
			minDepth = md;
		for (Hypocenter hc: hypocenters) {
			minDepth = Math.min(minDepth, hc.depth);
		}
		return minDepth;
	}
	
	/**
	 * Get deepest depth in the list
	 * @return max depth in list
	 */
	public double getMaxDepth(double md)
	{
		double maxDepth = -1.0;
		if (md != Double.MAX_VALUE)
			maxDepth = md;
		for (Hypocenter hc: hypocenters) {
			maxDepth = Math.max(maxDepth, hc.depth);
		}
		return maxDepth;
	}
	
	/**
	 * Dump object content into ByteBuffer
	 * @return ByteBuffer of content
	 */
	public ByteBuffer toBinary()
	{
		ByteBuffer buffer = ByteBuffer.allocate(4 + hypocenters.size() * 17 * 8);
		buffer.putInt(hypocenters.size());
		for (Hypocenter hc : hypocenters)
			hc.insertIntoByteBuffer(buffer);
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
		hypocenters = new ArrayList<Hypocenter>(rows);
		for (int i = 0; i < rows; i++) {
			Hypocenter hc = new Hypocenter(bb.getDouble(), (String)null, bb.getInt(), bb.getDouble(), bb.getDouble(), bb.getDouble(), bb.getDouble(),
						bb.getDouble(), bb.getDouble(), bb.getInt(), bb.getInt(), bb.getDouble(), bb.getDouble(), bb.getInt(),
						bb.getDouble(), bb.getDouble(), Character.toString ((char) bb.getInt()), Character.toString ((char) bb.getInt()) );
			hypocenters.add(hc);
		}
	}
	
	/**
	 * Get string representation of hypocenters list
	 * @return string representation
	 */
	public String toString()
	{
		return "HypocenterList: " + hypocenters.size() + " hypocenters";
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
		DoubleMatrix2D result = DoubleFactory2D.dense.make(hypocenters.size(), 2);
		for (int i = 0; i < hypocenters.size(); i++)
		{
			Hypocenter hc = hypocenters.get(i);
			result.setQuick(i, 0, hc.j2ksec);
			result.setQuick(i, 1, i);
		}
		return result;
	}
	
	/**
	 * Compute 2-column matrix: time - cumulative moment
	 * @return matrix
	 */
	public DoubleMatrix2D getCumulativeMoment()
	{
		DoubleMatrix2D result = DoubleFactory2D.dense.make(hypocenters.size(), 2);
		for (int i = 0; i < hypocenters.size(); i++)
		{
			Hypocenter hc = hypocenters.get(i);
			result.setQuick(i, 0, hc.j2ksec);
			double mo = Math.pow(10, (321 / 20 + 3 * hc.prefmag / 2));
			if (i == 0)
				result.setQuick(i, 1, mo);
			else
				result.setQuick(i, 1, result.getQuick(i - 1, 1) + mo);
		}
		return result;
	}

	/**
	 * Compute 2-column matrix: time - cumulative magnitude
	 * @return matrix
	 */
	public DoubleMatrix2D getCumulativeMagnitude()
	{
		DoubleMatrix2D result = getCumulativeMoment();
		double log10 = Math.log(10);
		for (int i = 0; i < result.rows(); i++)
		{
			result.setQuick(i, 1, Math.log(result.getQuick(i, 1)) / log10 / 1.5 - 10.7);
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
		if (hypocenters == null)
			return null;
		
		Histogram1D hist = new Histogram1D("", getHistogramAxis(bin));
		for (Hypocenter hc : hypocenters)
			hist.fill(hc.j2ksec);
		return hist;
	}
	
	/**
	 * Project of all hypocenters in the list
	 * @param proj Projection
	 */
	public void project(Projection proj)
	{
		for (Hypocenter hc : hypocenters)
			hc.project(proj);
	}
	
	/**
	 * Adds a value to the time (for time zone management).
	 * @param adj the time adjustment
	 */
	public void adjustTime(double adj)
	{
		for (Hypocenter hc : hypocenters)
			hc.j2ksec+=adj;
	}
	
	/**
	 * Dump list as CSV string
	 * @return string representation in CSV
	 */
	public String toCSV()
	{
		StringBuffer sb = new StringBuffer();
		for (Hypocenter hc : hypocenters)
			sb.append(hc.toString() + "\n");
		
		return sb.toString();
	}
}
