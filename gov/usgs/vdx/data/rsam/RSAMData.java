package gov.usgs.vdx.data.rsam;

import gov.usgs.util.Util;
import gov.usgs.vdx.data.GenericDataMatrix;
import gov.usgs.vdx.data.hypo.HypocenterList.BinSize;
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
 * A class that deals with RSAM data.  The data are stored in a 2-D matrix, the
 * first column is the time (j2ksec), the second is the data.
 *
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * Revision 1.2  2005/04/07 22:54:04  cervelli
 * Added ByteBuffer functions and List based constructor.
 *
 * Revision 1.1  2004/10/12 18:21:25  cvs
 * Initial commit.
 *
 * @author Dan Cervelli
 */
public class RSAMData extends GenericDataMatrix
{
	
	private static final int MAX_BINS = 1000000;
	DoubleMatrix2D events;

	/** Generic empty constructor
	 */
	public RSAMData()
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
	public RSAMData(ByteBuffer bb)
	{
		super(bb);
	}

	public RSAMData(List<double[]> list)
	{
		super(list);
	}

	/** Gets the RSAM column (column 2) of the data. 
	 * @return the data column
	 */
	public DoubleMatrix2D getRSAM()
	{
		return data.viewPart(0, 1, rows(), 1);
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

	public void countEvents(double threshold, double ratio, double maxLength)
	{
		double oldValue = 0;
		double olderValue = 0;
		boolean eventOngoing = false;
		double eventStart = 0;
		int eventCount = 0;
		double[] eventTimes = new double[data.rows()];
		
		for (int i=1; i<data.rows(); i++)
		{
			double currentTime = data.get(i, 0);
			double currentValue = data.get(i, 1);

			if (currentValue >= threshold && currentValue >= olderValue * ratio)
			{
				if (currentTime - eventStart > maxLength)
				{
					eventOngoing = false;
					eventStart = currentTime;
				}
				else if (eventOngoing == false)
				{
					eventStart = currentTime;
					eventOngoing = true;
					eventTimes[eventCount++] = currentTime;
				}
			} else {
				eventOngoing = false;
				eventStart = currentTime;
			}
			
			olderValue = oldValue;
			oldValue = currentValue;
		}
		
		events = DoubleFactory2D.dense.make(eventCount+2, 2);
		events.setQuick(0, 0, data.get(0,0));
		events.setQuick(0, 1, 0);
		
		int c = 1;
		for (int i=1; i<=eventCount; i++)
		{
			events.setQuick(i, 0, eventTimes[i-1]);
			events.setQuick(i, 1, c++);
		}
		
		events.setQuick(eventCount+1, 0, data.get(data.rows()-1,0));
		events.setQuick(eventCount+1, 1, eventCount);
System.out.println("eventCount " + eventCount);
	}
	
	public DoubleMatrix2D getCumulativeCounts()
	{		
		return events;
	}
	
	public Histogram1D getCountsHistogram(BinSize bin)
	{
		if (data == null || data.size() == 0)
			return null;
		
		Histogram1D hist = new Histogram1D("", getHistogramAxis(bin));
		
		for (int i=1; i<events.rows()-1; i++)
		{
			hist.fill(events.get(i,0));
		}
		
		return hist;
	}
}