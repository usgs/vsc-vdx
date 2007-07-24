package gov.usgs.vdx.data;

import gov.usgs.util.Util;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.7  2006/12/05 22:05:23  tparker
 * fix 1 off bug in fillSparse
 *
 * Revision 1.6  2006/09/21 18:41:02  tparker
 * kludge to deal with sparse data
 *
 * Revision 1.5  2006/09/14 18:09:38  tparker
 * Add sum method for cumulative plotting
 *
 * Revision 1.4  2006/01/10 20:55:04  tparker
 * Add start/end time methods
 *
 * Revision 1.3  2005/12/23 02:09:36  tparker
 * Add export method for RawData export
 *
 * Revision 1.2  2005/09/06 21:36:19  dcervelli
 * Added min(), mean(), max().
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class GenericDataMatrix implements BinaryDataSet
{
	protected DoubleMatrix2D data;
	protected HashMap<String, Integer> columnMap;
	
	public GenericDataMatrix()
	{
		data = null;
		columnMap = new HashMap<String, Integer>();
		setColumnNames();
	}
	
	public GenericDataMatrix(DoubleMatrix2D d)
	{
		this();
		data = d;
	}
	
	/**
	 * Create a GenericDataMatrix from a byte buffer.  
	 * 
	 * @param bb the byte buffer
	 */
	public GenericDataMatrix(ByteBuffer bb)
	{
		this();
		fromBinary(bb);
	}
	
	public GenericDataMatrix(List<double[]> list)
	{
		this();
		if (list == null || list.size() == 0)
			return;
		
		int rows = list.size();
		int cols = list.get(0).length;
		
		data = DoubleFactory2D.dense.make(rows, cols);
		for (int i = 0; i < rows; i++)
		{
			double[] d = (double[])list.get(i);
			for (int j = 0; j < cols; j++)
				data.setQuick(i, j, d[j]);
		}
	}
	
	public ByteBuffer toBinary()
	{
		int rows = rows();
		int cols = columns();
		ByteBuffer bb = ByteBuffer.allocate(4 + (rows * cols) * 8);
		bb.putInt(rows);
		for (int i = 0; i < rows; i++)
		{
			for (int j = 0; j < cols; j++)
				bb.putDouble(data.getQuick(i, j));
		}
		return bb;
	}
		
	public void fromBinary(ByteBuffer bb)
	{
		int rows = bb.getInt();
		int cols = ((bb.limit() - 4) / rows) / 8;
		data = DoubleFactory2D.dense.make(rows, cols);
		for (int i = 0; i < rows; i++)
		{
			for (int j = 0; j < cols; j++)
				data.setQuick(i, j, bb.getDouble());
		}		
	}
	
	public String toCSV()
	{
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < rows(); i++)
		{
			sb.append(Util.j2KToDateString(data.getQuick(i, 0)) + ",");
			for (int j = 1; j < columns(); j++)
			{
				sb.append(data.getQuick(i, j));
				sb.append(",");
			}
			sb.append("\n");
		}
		return sb.toString();
	}
	
	/** Sets the data matrix.
	 * @param d the data
	 */
	public void setData(DoubleMatrix2D d)
	{
		data = d;
	}

	public void setColumnNames()
	{}

	public void setColumnNames(String[] s)
	{
		int i = 0;
		for (String name : s)
			columnMap.put(name, i++);
	}
	
	/** Gets the number of rows in the data.
	 * @return the row count
	 */
	public int rows()
	{
		if (data != null)
			return data.rows();
		else
			return 0;
	}
	
	public int columns()
	{
		if (data != null)
			return data.columns();
		else 
			return 0;
	}

	public void add(String c, double v)
	{
		Integer i = columnMap.get(c);
		if (i != null)
			add(i, v);
	}
	
	public void add(int c, double v)
	{
		for (int i = 0; i < rows(); i++)
			data.setQuick(i, c, data.getQuick(i, c) + v);
	}
	
	public void mult(String c, double v)
	{
		Integer i = columnMap.get(c);
		if (i != null)
			mult(i, v);
	}
	
	public void mult(int c, double v)
	{
		for (int i = 0; i < rows(); i++)
			data.setQuick(i, c, data.getQuick(i, c) * v);
	}

	public void sum(int c)
	{
		for (int i=1; i<rows(); i++)
		{
			double d = data.getQuick(i-1,c);
			d += data.getQuick(i,c);
			data.setQuick(i,c,d);
		}
	}
	
	public double max(int c)
	{
		double m = -1E300;
		for (int i = 0; i < rows(); i++)
			m = Math.max(m, data.getQuick(i, c));
		return m;
	}
	
	public double min(int c)
	{
		double m = 1E300;
		for (int i = 0; i < rows(); i++)
			m = Math.min(m, data.getQuick(i, c));
		return m;
	}
	
	public double mean(int c)
	{
		double t = 0;
		for (int i = 0; i < rows(); i++)
			t += data.getQuick(i, c);
		return t / (double)rows();
	}
	
	public double getStartTime()
	{
		
		if (data == null || data.size() == 0)
			return Double.NaN;
		else
			return data.get(0,0);
	}
	
	public double getEndTime()
	{
		if (data == null || data.size() == 0)
			return Double.NaN;
		else
			return data.get(rows()-1,0);
	}
	
	/** Adds a value to the time column (for time zone management).
	 * @param adj the time adjustment
	 */
	public void adjustTime(double adj)
	{
		add(0, adj);
	}

	/** Gets the time column (column 1) of the data.
	 * @return the time column
	 */
	public DoubleMatrix2D getTimes()
	{
		return data.viewPart(0, 0, rows(), 1);
	}

	/** Gets a data column. 
	 * @return the data column
	 */
	public DoubleMatrix2D getColumn(int c)
	{
		return data.viewPart(0, c, rows(), 1);
	}
	
	public DoubleMatrix2D getColumn(String c)
	{
		Integer i = columnMap.get(c);
		if (i != null)
			return getColumn(i);
		else 
			return null;
	}

	/** Gets the data matrix.
	 * @return the data
	 */
	public DoubleMatrix2D getData()
	{
		return data;
	}

	public void concatenate(GenericDataMatrix dm)
	{
		DoubleMatrix2D[][] ms = new DoubleMatrix2D[2][1];
		ms[0][0] = data;
		ms[1][0] = dm.getData();
		data = DoubleFactory2D.dense.compose(ms);
	}
	
	public int getMemorySize()
	{
		return (data.rows() * data.columns() * 8);	
	}

}
