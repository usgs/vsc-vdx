package gov.usgs.vdx.data.heli;

import gov.usgs.util.Log;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.GenericDataMatrix;
import gov.usgs.vdx.data.wave.Wave;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

/**
 * HelicorderData is a class that stores the information that makes up a
 * helicorder: 1-sec min/max data.
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * Revision 1.9  2005/04/23 15:52:21  cervelli
 * Removed the average rcnt constructor.
 *
 * Revision 1.8  2005/04/17 16:34:23  cervelli
 * Multiple optimizations.
 *
 * Revision 1.7  2005/04/14 15:36:51  cervelli
 * Small change to subset().
 *
 * Revision 1.6  2005/04/10 00:25:59  cervelli
 * Eliminated last remains of compression code.
 *
 * Revision 1.5  2005/03/31 02:38:21  cervelli
 * Added new constructor for dealing with variable WWS rcnt values.
 *
 * Revision 1.4  2005/03/27 18:56:55  cervelli
 * Added list-based constructor, toString(), and some comments.
 *
 * Revision 1.3  2005/03/26 17:32:02  cervelli
 * Using Util.compress and Util.decompress instead of custom functions here.
 * Should probably eliminate the compression functions altogether.
 *
 * Revision 1.2  2005/03/25 00:15:14  cervelli
 * Compression/decompression and one data point per row ResultSet constructor.
 *
 * @author Dan Cervelli
 */
public class HelicorderData extends GenericDataMatrix
{
//	private DoubleMatrix2D data;
	protected final static Logger logger = Log.getLogger("gov.usgs.vdx.data.heli.HelicorderData"); 
	private transient double bias = -1E300;
	private transient double meanMax = -1E300;
	
	/**
	 * Set predefined column names: time/min/max
	 */
	public void setColumnNames()
	{
		columnMap.put("time", 0);
		columnMap.put("min", 1);
		columnMap.put("max", 2);
	}
	
	/**
	 * Generic empty constructor.
	 */
	public HelicorderData() {}

	/**
	 * Create a helicorder from a byte buffer.  This first 4 bytes specify an
	 * integer number of rows followed by rows*24 bytes, 3 doubles: j2ksec, min, 
	 * max.
	 * 
	 * @param bb the byte buffer
	 */
	public HelicorderData(ByteBuffer bb)
	{
		super(bb);
	}
	
	/**
	 * Creates a helicorder from a double[] (j2ksec, min, max) list.
	 *  
	 * @param list the list of values
	 */
	public HelicorderData(List<double[]> list)
	{
		super(list);
	}
	
	/**
	 * Reset bias max and min values to undefined state
	 */
	public void resetBiasMinMax()
	{
		bias = -1E300;
		meanMax = -1E300;
	}
	
	/** Gets the min value column (column 2) of the data. 
	 * @return the data column
	 */
	public DoubleMatrix2D getMin()
	{
		return data.viewPart(0, 1, rows(), 1);
	}
	
	/** Gets the max value column (column 3) of the data. 
	 * @return the data column
	 */
	public DoubleMatrix2D getMax()
	{
		return data.viewPart(0, 2, rows(), 1);
	}
	
	/**
	 * Get bias for all data
	 * @return the bias
	 */
	public double getBias()
	{
		if (bias != -1E300)
			return bias;
		
		double b = 0;
		int samples = 0;
		int numRows = data.rows();
		double d1, d2;
		for (int i = 0; i < numRows; i++)
		{
			d1 = data.getQuick(i, 1);
			d2 = data.getQuick(i, 2);
			if (d1 != Wave.NO_DATA && d2 != Wave.NO_DATA)
			{
				b += d2 - ((d2 - d1) / 2.0d);
				samples++;
			}
		}
			
		b /= samples;
		bias = b;
		return bias;
	}
	
	/**
	 * Find index for data
	 * @param t time to search index
	 * @param left start index value
	 * @param right end index value
	 * @return the index
	 */
	private int findIndex(double t, int left, int right)
	{
		int mid = 0;
		double val;
		while (left <= right)
		{
			mid = (left + right) / 2;
			val = data.getQuick(mid, 0);
			if (val == t)
				return mid;
			else if (t < val)
				right = mid - 1;
			else if (t > val)
				left = mid + 1;
		}
		return mid;
	}
	
	/**
	 * 
	 * Get bias for data between t1 and t2 time values
	 * @param t1 start time for range to get bias for
	 * @param t2 end time for range to get bias for
	 * @return the bias for t2..t2
	 */
	public double getBiasBetween(double t1, double t2)
	{
		double bias = 0;
		int samples = 0;

		int m = findIndex(t1, 0, data.rows() - 1);
		int n = findIndex(t2, 0, data.rows() - 1);
		
		double d1, d2;
		for (int i = m; i < n; i++)
		{
			d1 = data.getQuick(i, 1);
			d2 = data.getQuick(i, 2);
			if (d1 != Wave.NO_DATA && d2 != Wave.NO_DATA)
			{
				bias += d2 - ((d2 - d1) / 2.0d);
				samples++;
			}
		}
		
		bias /= samples;
		return bias;	
	}
	
	/**
	 * Split whole time range on raw set with duration timeChunk.
	 * Get array of biases for each row.
	 * @param timeChunk duration
	 * @return array of biases
	 */
	public double[] getBiasByRow(double timeChunk)
	{
		double mint = getStartTime() - (getStartTime() % timeChunk);
		double maxt = getEndTime() + ((timeChunk - (getEndTime() % timeChunk)));
		
		int rows = (int)((maxt - mint) / timeChunk);
		double[] biases = new double[rows];
		double rowStartTime = mint;
		int samples = 0;
		int row = 0;
		double t, d1, d2;
		for (int i = 0; i < data.rows(); i++)
		{
			t = data.getQuick(i, 0);
			d1 = data.getQuick(i, 1);
			d2 = data.getQuick(i, 2);
			if (t > rowStartTime + timeChunk)
			{
				biases[row] /= samples;
				logger.fine(row + " " + biases[row]);
				row++;
				samples = 0;
				rowStartTime += timeChunk;
			}
			
			if (d1 != Wave.NO_DATA && d2 != Wave.NO_DATA)
			{
				biases[row] += d2 - (d2 - d1) / 2;
				samples++;
			}
		}
		
		return biases;
	}
	
	/**
	 * Remove bias from data 
	 * @return removed bias
	 */
	public double removeBias()
	{
		double bias = 0;
		for (int i = 0; i < data.rows(); i++)
			bias += (data.getQuick(i, 2) - data.getQuick(i, 1)) / 2;
			
		bias /= data.rows();
		for (int i = 0; i < data.rows(); i++)
		{
			data.setQuick(i, 2,	data.getQuick(i, 2) - bias);
			data.setQuick(i, 1,	data.getQuick(i, 1) - bias);
		}
		return bias;		
	}
	
	/**
	 * Get mean for value section max values on whole data
	 * @return mean of maxes
	 */
	public double getMeanMax()
	{
		if (meanMax != -1E300)
			return meanMax;
		
		double mean = 0;
		int samples = 0;
		double d;
		int numRows = data.rows();
		for (int i = 0; i < numRows; i++)
		{
			d = data.getQuick(i, 2);
			if (d != Wave.NO_DATA)
			{
				mean += d;
				samples++;
			}
		}

		meanMax = mean / samples;
		return meanMax;	
	}
	
	/**
	 * Get mean of value section center position on whole data range
	 * @return mean of range
	 */
	public double getMeanRange()
	{
		double mean = 0;
		int samples = 0;
		for (int i = 0; i < data.rows(); i++)
		{
			mean += data.getQuick(i, 2) - data.getQuick(i, 1);
   			samples++;
		}
		
		return mean / samples;
	}
	
	/**
	 * Get start time
	 * @return start time
	 */
	public double getStartTime()
	{
		return data.getQuick(0, 0);
	}	
	
	/**
	 * Get end time
	 * @return end time
	 */
	public double getEndTime()
	{
		return Math.round(data.getQuick(rows() - 1, 0));	
	}
	
	/**
	 * Get flag if this helicorder data have time intersection with another
	 * @param heli HelicorderData to compare to this
	 * @return true if heli overlaps time range of this
	 */
	public boolean overlaps(HelicorderData heli)
	{
		// obviously this could be compressed to one line, but this is readable:
		// either the new wave is completely right of, completely left of or
		// overlapping the old wave
		if (getEndTime() < heli.getStartTime())
			return false;
			
		if (getStartTime() > heli.getEndTime())
			return false;
			
		return true;
	}
	
	/**
	 * Yield index of datum w/ smallest time >= time
	 * @param time lower bound of times to consider
	 * @return index of time found; -1 if none found
	 */
	public int findClosestTimeIndexGreaterThan(double time)
	{
		for (int i = 0; i < rows(); i++)
		{
			if (data.getQuick(i, 0) >= time)
				return i;
		}	
		return -1;
	}
	
	/**
	 * Yield index of datum w/ largest time <= time
	 * @param time upper bound of times to consider
	 * @return index of time found; -1 if none found
	 */
	public int findClosestTimeIndexLessThan(double time)
	{
		for (int i = rows() - 1; i >= 0; i--)
		{
			if (data.getQuick(i, 0) <= time)
				return i;
		}	
		return -1;
	}

	/**
	 * Get subset of data
	 * @param t1 start time 
	 * @param t2 end time
	 * @return HelicorderData within time range
	 */
	public HelicorderData subset(double t1, double t2)
	{
		int i1 = findClosestTimeIndexGreaterThan(t1);
		int i2 = findClosestTimeIndexLessThan(t2);
		if (i1 == -1 || i2 == -1 || i2 - i1 == 0)
			return null;
		DoubleMatrix2D[][] ms = new DoubleMatrix2D[1][1];
		ms[0][0] = data.viewPart(i1, 0, i2 - i1, 3);
		HelicorderData hd = new HelicorderData();
		hd.data = DoubleFactory2D.dense.compose(ms);
		return hd;
	}
	
	// can trash either helicorder
	/**
	 * Merge helicorders
	 * @param heli HelicorderData to merge into this
	 * @return this is successful, null otherwise
	 */
	public HelicorderData combine(HelicorderData heli)
	{
		// if (!overlaps(heli))
		//		return null;
		
		// other wave dominates this wave
		if (getStartTime() >= heli.getStartTime() && getEndTime() <= heli.getEndTime())
			return heli;
			
		// this wave dominates other wave
		if (getStartTime() <= heli.getStartTime() && getEndTime() >= heli.getEndTime())
			return this;
		
		// this wave is left of other wave	
		if (getStartTime() <= heli.getStartTime())
		{
			//logger.fine("rows before: " + rows());
			DoubleMatrix2D[][] ms = new DoubleMatrix2D[2][1];
			ms[0][0] = data;
			int i = heli.findClosestTimeIndexGreaterThan(getEndTime());
			ms[1][0] = heli.getData().viewPart(i, 0, heli.rows() - i, 3);
			data = DoubleFactory2D.dense.compose(ms);
			//System.out.println("combine l: " + data.rows() + " " + data.columns());
			return this;
		}
		
		// this wave is right of other wave
		if (heli.getStartTime() <= getStartTime())
		{
			//logger.fine("rows before: " + rows());
			DoubleMatrix2D[][] ms = new DoubleMatrix2D[2][1];
			ms[0][0] = heli.getData();
			int i = findClosestTimeIndexLessThan(heli.getEndTime());
			if (i == -1)
				i = 0;
			ms[1][0] = getData().viewPart(i, 0, rows() - i, 3);
			data = DoubleFactory2D.dense.compose(ms);
			//logger.fine("combine r: " + data.rows() + " " + data.columns());
			return this;
		}
		
		//logger.fine("unknown case");
		return null;
	}
	
	/**
	 * Sort helicorder data by time
	 */
	public void sort()
	{
		if (data == null)
			return;
		double[][] matrix = data.toArray();
		Arrays.sort(matrix, new Comparator<double[]>()
				{
					public int compare(double[] o1, double[] o2)
					{
						return Double.compare(o1[0], o2[0]);
					}
				});
		data = DoubleFactory2D.dense.make(matrix);
	}

	/**
	 * Get string representation, dump helicorder data matrix separating values by ' '
	 * @return string representation of this
	 */
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < rows(); i++)
		{
			for (int j = 0; j < 3; j++)
			{
				sb.append(data.getQuick(i, j));
				sb.append(" ");
			}
			sb.append("\n");
		}
		return sb.toString();
	}
	
	/**
	 * Dump helicorder data matrix as CSV
	 * @return string representation of this in CSV
	 */
	public String toCSV()
	{
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < rows(); i++)
		{
			sb.append(Util.j2KToDateString(data.getQuick(i,0)) + ",");
			sb.append(data.getQuick(i,1) + ",");
			sb.append(data.getQuick(i,2) + "\n");
		}
		return sb.toString();
	}
}