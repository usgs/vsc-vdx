package gov.usgs.vdx.data.hypo;

import gov.usgs.proj.Projection;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.BinaryDataSet;
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
 * 
 * $Log: not supported by cvs2svn $
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
		MINUTE("Minute"), HOUR("Hour"), DAY("Day"), WEEK("Week"), MONTH("Month"), YEAR("Year");
		
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

	public HypocenterList()
	{
		this(new ArrayList<Hypocenter>(1));
	}
	
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
	
	public int size()
	{
		return hypocenters.size();
	}
	
	public double getStartTime()
	{
		if (hypocenters == null || hypocenters.size() == 0)
			return Double.NaN;
		else
			return hypocenters.get(0).getTime();
	}
	
	public double getEndTime()
	{
		if (hypocenters == null || hypocenters.size() == 0)
			return Double.NaN;
		else
			return hypocenters.get(hypocenters.size() - 1).getTime();
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
	
	public DoubleMatrix2D getCumulativeCounts()
	{
		DoubleMatrix2D result = DoubleFactory2D.dense.make(hypocenters.size(), 2);
		for (int i = 0; i < hypocenters.size(); i++)
		{
			Hypocenter hc = hypocenters.get(i);
			result.setQuick(i, 0, hc.getTime());
			result.setQuick(i, 1, i);
		}
		return result;
	}
	
	public DoubleMatrix2D getCumulativeMoment()
	{
		DoubleMatrix2D result = DoubleFactory2D.dense.make(hypocenters.size(), 2);
		for (int i = 0; i < hypocenters.size(); i++)
		{
			Hypocenter hc = hypocenters.get(i);
			result.setQuick(i, 0, hc.getTime());
			double mo = Math.pow(10, (321 / 20 + 3 * hc.getMag() / 2));
			if (i == 0)
				result.setQuick(i, 1, mo);
			else
				result.setQuick(i, 1, result.getQuick(i - 1, 1) + mo);
		}
		return result;
	}
	
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
	
	public Histogram1D getCountsHistogram(BinSize bin)
	{
		if (hypocenters == null || hypocenters.size() == 0)
			return null;
		
		Histogram1D hist = new Histogram1D("", getHistogramAxis(bin));
		for (Hypocenter hc : hypocenters)
			hist.fill(hc.getTime());
		return hist;
	}
	
	public void project(Projection proj)
	{
		for (Hypocenter hc : hypocenters)
			hc.project(proj);
	}
}
