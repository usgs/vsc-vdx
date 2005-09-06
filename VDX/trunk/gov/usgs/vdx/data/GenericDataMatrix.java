package gov.usgs.vdx.data;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

/**
 * 
 * $Log: not supported by cvs2svn $
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
	
	/** Sets the data matrix.
	 * @param d the data
	 */
	public void setData(DoubleMatrix2D d)
	{
		data = d;
	}

	public void setColumnNames()
	{}
	
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
