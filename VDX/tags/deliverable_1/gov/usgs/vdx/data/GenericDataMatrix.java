package gov.usgs.vdx.data;

import gov.usgs.util.Util;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

/**
 * BinaryDataSet to store cern's DoubleMatrix2D and meta information about matrix column's names
 *
 * @author Dan Cervelli
 */
public class GenericDataMatrix implements BinaryDataSet
{
	protected DoubleMatrix2D data;
	protected HashMap<String, Integer> columnMap;
	
	/**
	 * Default constructor
	 */
	public GenericDataMatrix()
	{
		data = null;
		columnMap = new HashMap<String, Integer>();
		setColumnNames();
	}
	
	/**
	 * Construct GenericDataMatrix from given 2d matrix
	 * @param d
	 */
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

	/**
	 * Create a GenericDataMatrix from a List<double[]>.  
	 * 
	 */
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
			double[] d = list.get(i);
			for (int j = 0; j < cols; j++)
				data.setQuick(i, j, d[j]);
		}
	}
	
	/**
	 * Dumps content as ByteBuffer
	 */
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
	
	/**
	 * Init content from ByteBuffer
	 */
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
	
	/**
	 * Dumps content as CSV
	 */
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

	/**
	 * Sets names of matrix conumns
	 */
	public void setColumnNames()
	{}


	/**
	 * Sets names of matrix conumns
	 * @param s Array of strings - column names
	 */
	public void setColumnNames(String[] s)
	{
		int i = 0;
		for (String name : s)
			columnMap.put(name, i++);
	}

	/**
	 * Gets names of matrix conumns
	 * @return Array of strings - column names
	 */
	public String[] getColumnNames()
	{
		String[] c = new String[columnMap.size()];
		for (String s : columnMap.keySet())
			c[columnMap.get(s)] = s;
		
		return c;
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

	/** Gets the number of columns in the data.
	 * @return the column count
	 */
	public int columns()
	{
		if (data != null)
			return data.columns();
		else 
			return 0;
	}

	/**
	 * Add double value to one column
	 * @param c column name to add
	 * @param v value to add
	 */
	public void add(String c, double v)
	{
		Integer i = columnMap.get(c);
		if (i != null)
			add(i, v);
	}
	
	/**
	 * Add double value to one column
	 * @param c column number to add
	 * @param v value to add
	 */
	public void add(int c, double v)
	{
		for (int i = 0; i < rows(); i++) {
			if (!Double.isNaN(data.getQuick(i, c))) {
				data.setQuick(i, c, data.getQuick(i, c) + v);
			}
		}
	}

	/**
	 * Multiply one column to double value
	 * @param c name of column to multiply
	 * @param v value to multiply
	 */
	public void mult(String c, double v)
	{
		Integer i = columnMap.get(c);
		if (i != null)
			mult(i, v);
	}

	/**
	 * Multiply one column to double value
	 * @param c number of column to multiply
	 * @param v value to multiply
	 */
	public void mult(int c, double v)
	{
		for (int i = 0; i < rows(); i++) {
			if (!Double.isNaN(data.getQuick(i, c))) {
				data.setQuick(i, c, data.getQuick(i, c) * v);
			}
		}
	}

	/**
	 * Sums column, value in resulting column is sum of all previous raws.
	 * @param c
	 */
	public void sum(int c)
	{
		for (int i=1; i<rows(); i++)
		{
			double d = data.getQuick(i-1,c);
			d += data.getQuick(i,c);
			data.setQuick(i,c,d);
		}
	}

	public void detrend(int c) {

        double xm	= mean(0);
		double ym	= mean(c);
        double ssxx	= 0;
        double ssxy	= 0;        
        for (int i = 0; i < rows(); i++) {
			if (!Double.isNaN(data.getQuick(i, c))) {
				ssxy += (data.getQuick(i, 0) - xm) * (data.getQuick(i, c) - ym);
				ssxx += (data.getQuick(i, 0) - xm) * (data.getQuick(i, 0) - xm);
			}
        }
        
        double m	= ssxy / ssxx;
        double b	= ym - m * xm;
        for (int i = 0; i < rows(); i++) {
			if (!Double.isNaN(data.getQuick(i, c))) {
				data.setQuick(i, c, data.getQuick(i, c) - (data.getQuick(i, 0) * m + b));
			}
        }
	}
	
	/**
	 * Get maximum value in column
	 * @param c column number
	 */
	public double max(int c)
	{
		double m = -1E300;
		for (int i = 0; i < rows(); i++) {
			if (!Double.isNaN(data.getQuick(i, c))) {
				m = Math.max(m, data.getQuick(i, c));
			}
		}
		return m;
	}

	/**
	 * Get minimum value in column
	 * @param c column number
	 */
	public double min(int c)
	{
		double m = 1E300;
		for (int i = 0; i < rows(); i++) {
			if (!Double.isNaN(data.getQuick(i, c))) {
				m = Math.min(m, data.getQuick(i, c));
			}
		}
		return m;
	}
	
	/**
	 * Get mean value in column
	 * @param c column number
	 */
	public double mean(int c)
	{
		double t = 0;
		double j = 0;
		for (int i = 0; i < rows(); i++) {
			if (!Double.isNaN(data.getQuick(i, c))) {
				t += data.getQuick(i, c);
				j++;
			}
		}
		return t / j;
	}
    
	/** Returns the least squares fit line from a column.  Data are returned
	 * as a double array, first element slope, second element y-intercept.
	 * NEEDS SUPPORT FOR NO_DATA!
	 * @param column the column index
	 * @return the slope and y-intercept of the line
	 */
    public double[] leastSquares(int c)
    {
        double ym = mean(c);
        double xm = mean(0);
        
        double ssxy = 0;
        double ssxx = 0;
        for (int i = 0; i < data.rows(); i++)
        {
        	// Ask Asta about this approach
        	if (!Double.isNaN(data.getQuick(i, c))) {
        		ssxy += (data.getQuick(i, 0) - xm) * (data.getQuick(i, c) - ym);
        		ssxx += (data.getQuick(i, 0) - xm) * (data.getQuick(i, 0) - xm);
        	}
        }
        double m = ssxy / ssxx;
        double b = ym - m * xm;
        return new double[] {m, b};
    }
	
	/**
	 * @return (0,0) value of matrix
	 */
	public double getStartTime()
	{		
		if (data == null || data.size() == 0)
			return Double.NaN;
		else
			return data.get(0,0);
	}

	/**
	 * @return (rows()-1,0) value of matrix
	 */
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
	
	/**
	 * Gets a data column.
	 * @param c Column name
	 * @return the data column
	 */
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

	/**
	 * Contatenate two matrix
	 * @param dm matrix to concatenate with this one
	 */
	public void concatenate(GenericDataMatrix dm)
	{
		DoubleMatrix2D[][] ms = new DoubleMatrix2D[2][1];
		ms[0][0] = data;
		ms[1][0] = dm.getData();
		data = DoubleFactory2D.dense.compose(ms);
	}
	
	/**
	 * 
	 * @return size of memory occuped by data matrix, in bytes
	 */
	public int getMemorySize()
	{
		return (data.rows() * data.columns() * 8);	
	}
}