package gov.usgs.vdx.data.zeno;

import java.sql.*;
import cern.colt.matrix.*;

/**
 * A class that holds tilt environment data.  The data are stored in a 2-D matrix, the
 * first column is time (j2ksec), the second is hole temperature, the 
 * third is box temperature, the fourth is data logger battery voltage, and 
 * the fifth is accumulated rainfall over the 24 hour period. 
 *
 * $Log: not supported by cvs2svn $
 * 
 * @@author Dan Cervelli
 * @@version 2.00
 */
public class TiltEnvData
{
	/** Column number for time.
	 */
    public static final int J2KSEC = 0;
	/** Column number for hole temperature.
	 */
    public static final int HOLETEMP = 1;
	/** Column number for box temperature.
	 */
    public static final int BOXTEMP = 2;
	/** Column number for data logger battery voltage.
	 */
    public static final int VOLTAGE = 3;
	/** Column number for accumulated 24-hour rainfall.
	 */
    public static final int RAINFALL = 4;
	
    private DoubleMatrix2D envData;

	/** Generic empty constructor.
	 */
	public TiltEnvData() {}
	
    /** Constructor that creates data from a database query.  Technically this 
	 * should be part of the SQL implemenation, but in order to optimize it
	 * was put directly here.  The ResultSet's first five columns should be 
	 * j2ksec, hole temp, box temp, voltage, and rainfall.
	 * @@param rows the number of rows of data
	 * @@param rs the data 
	 */
    public TiltEnvData(int rows, ResultSet rs)
    {
        try
        {
            envData = DoubleFactory2D.dense.make(rows, 5);
            for (int i = 0; i < rows; i++)
            {
                rs.next();
                envData.setQuick(i, 0, rs.getDouble(1));
                envData.setQuick(i, 1, rs.getDouble(2));
                envData.setQuick(i, 2, rs.getDouble(3));
                envData.setQuick(i, 3, rs.getDouble(4));
                envData.setQuick(i, 4, rs.getDouble(5));
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
		envData = d;
	}
	
	/** Gets the number of rows of data.
	 * @@return the number of rows
	 */
    public int rows()
    {
        return envData.rows();
    }
    
	/** Gets the data matrix.
	 * @@return the data matrix
	 */
	public DoubleMatrix2D getData()
	{
		return envData;
	}
	
	/** Synonym for getData().
	 * @@return the data matrix
	 */
    public DoubleMatrix2D getAllData()
    {
        return envData;
    }
    
	/** Gets an individual data column and pairs it with time.
	 * @@param set the data set column number
	 * @@return the Nx2 data matrix
	 */
    public DoubleMatrix2D getDataSet(int set)
    {
        DoubleMatrix2D t = envData.viewPart(0, 0, rows(), 1);
        DoubleMatrix2D d = envData.viewPart(0, set, rows(), 1);
        return DoubleFactory2D.dense.compose(new DoubleMatrix2D[][] {{t, d}});
    }
	
    /** Adds a value to the time column (for time zone management).
	 * @@param adj the time adjustment
	 */
    public void adjustTime(double adj)
    {
        for (int i = 0; i < envData.rows(); i++)
            envData.setQuick(i, 0, envData.getQuick(i, 0) + adj);
    }
    
	/** Adjusts the rain column so that it shows cumulative rainfall over the 
	 * full time interval.
	 */
    public void adjustRain()
    {
		double total = 0;
		double last = envData.getQuick(0, RAINFALL);
		double r;
		for (int i = 1; i < envData.rows(); i++)
		{
			r = envData.getQuick(i, RAINFALL);
			if (r < last)
				last = 0;

			total += (r - last);
			envData.setQuick(i, RAINFALL, total);
			last = r;
		}
    }
    
}