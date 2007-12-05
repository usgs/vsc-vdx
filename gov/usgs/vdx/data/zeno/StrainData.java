package gov.usgs.vdx.data.zeno;

import java.sql.*;
import cern.colt.matrix.*;

/**
 * A class that deals with strain data.  The data are stored in a 2-D matrix, the
 * first column is the time (j2ksec), the second is the dt01 component, and 
 * the third is the dt02 component.
 *
 * $Log: not supported by cvs2svn $
 * 
 * @@author Dan Cervelli
 * @@version 2.00
 */
public class StrainData
{
    private DoubleMatrix2D t12Data;

	/** Generic empty constructor.
	 */
	public StrainData() {}
	
	/** Constructor that creates data from a database query.  Technically this 
	 * should be part of the SQL implemenation, but in order to optimize it
	 * was put directly here.  The ResultSet's first three columns should be 
	 * j2ksec, dt01, and dt02.
	 * @@param rows the number of rows of data
	 * @@param rs the data 
	 */
    public StrainData(int rows, ResultSet rs)
    {
        try
        {
            t12Data = DoubleFactory2D.dense.make(rows, 3);
            for (int i = 0; i < rows; i++)
            {
                rs.next();
                t12Data.setQuick(i, 0, rs.getDouble(1));
                t12Data.setQuick(i, 1, rs.getDouble(2));
                t12Data.setQuick(i, 2, rs.getDouble(3));
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }
	
	/** Sets the data matrix.
	 * @@param d the data matrix
	 */
	public void setData(DoubleMatrix2D d)
	{
		t12Data = d;
	}
    
	/** Gets the number of rows of data.
	 * @@return the number of rows
	 */
    public int rows()
    {
        return t12Data.rows();
    }
    
	/** Gets the data matrix.
	 * @@return the data matrix
	 */
	public DoubleMatrix2D getData()
	{
		return t12Data;
	}
	
	/** Synonym for getData().
	 * @@return the data matrix
	 */
    public DoubleMatrix2D getAllData()
    {
        return t12Data;
    }
    
	/** Adds a value to the time column (for time zone management).
	 * @@param adj the time adjustment
	 */	
    public void adjustTime(double adj)
    {
        for (int i = 0; i < t12Data.rows(); i++)
            t12Data.setQuick(i, 0, t12Data.getQuick(i, 0) + adj);
    }
}