package gov.usgs.vdx.data.zeno;

import java.sql.*;
import cern.colt.function.*;
import cern.colt.matrix.*;
import cern.colt.matrix.linalg.*;

/**
 * A class that deals with tilt data.  The data are stored in a 2-D matrix, the
 * first column is the time (j2ksec), the second is the east component, and 
 * the third is the north component.
 *
 * <p>A set of functions is provided for orienting the data with respect to 
 * a specified or calculated azimuth.
 *
 * $Log: not supported by cvs2svn $
 *
 * @@author Dan Cervelli
 * @@version 2.00
 */
public class TiltData
{
    /** The time/east/north data matrix.
	 */
    private DoubleMatrix2D tenData;
    
	/** Generic empty constructor.
	 */
    public TiltData() {}
    
	/** Constructor that creates data from a database query.  Technically this 
	 * should be part of the SQL implemenation, but in order to optimize it
	 * was put directly here.  The ResultSet's first three columns should be 
	 * j2ksec, east, and north.
	 * @@param rows the number of rows of data
	 * @@param rs the data 
	 */
    public TiltData(int rows, ResultSet rs)
    {
        try
        {
            tenData = DoubleFactory2D.dense.make(rows, 3);
            for (int i = 0; i < rows; i++)
            {
                rs.next();
                tenData.setQuick(i, 0, rs.getDouble(1));
                tenData.setQuick(i, 1, rs.getDouble(2));
                tenData.setQuick(i, 2, rs.getDouble(3));
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }
	
    /** Gets the number of rows of data.
	 * @@return the number of rows
	 */
    public int rows()
    {
        return tenData.rows();
    }
    
	/** Get the time/east/north data for a specific row.
	 * @@param row the row number
	 * @@return t/e/n for that row
	 */
    public double[] getTEN(int row)
    {
        return new double[] {tenData.get(row, 0), tenData.get(row, 1), tenData.get(row, 2)};
    }

	/** Adds a value to the time column (for time zone management).
	 * @@param adj the time adjustment
	 */
    public void adjustTime(double adj)
    {
        for (int i = 0; i < tenData.rows(); i++)
            tenData.setQuick(i, 0, tenData.getQuick(i, 0) + adj);
    }
    
	/** Sets the data matrix.
	 * @@param d the data
	 */
    public void setData(DoubleMatrix2D d)
    {
        tenData = d;
    }

	/** Gets time/east/north/radial/tangential/magnitude/azimuth data using
	 * the specified azimuth.
	 * @@param theta azimuth in degrees
	 * @@return the Nx7 data matrix
	 */
    public DoubleMatrix2D getAllData(double theta)
    {
        return DoubleFactory2D.dense.compose(new DoubleMatrix2D[][] 
                {{tenData,
                  getRotatedDataWithoutTime(theta),
                  getVelocityDataWithoutTime()}});
    }
    
	/** Gets radial/tangential data using the specified azimuth.
	 * @@param theta the azimuth in degrees
	 * @@return the Nx2 data matrix
	 */
    public DoubleMatrix2D getRotatedDataWithoutTime(double theta)
    {
        DoubleMatrix2D en = tenData.viewPart(0, 1, tenData.rows(), 2);
        return Algebra.DEFAULT.mult(en, getRotationMatrix(theta));
    }
	
    /** Gets time/radial/tangential data using the specified azimuth.
	 * @@param theta the azimuth in degrees
	 * @@return the Nx3 data matrix
	 */
    public DoubleMatrix2D getRotatedData(double theta)
    {
        DoubleMatrix2D rt = getRotatedDataWithoutTime(theta);
        return DoubleFactory2D.dense.compose(new DoubleMatrix2D[][] {{tenData.viewPart(0, 0, tenData.rows(), 1), rt}});
    }
    
	/** Gets magnitude/azimuth data.
	 * @@return the Nx2 data matrix
	 */
    public DoubleMatrix2D getVelocityDataWithoutTime()
    {
        final DoubleMatrix2D en = tenData.viewPart(0, 1, tenData.rows(), 2);
        DoubleMatrix2D ma = en.copy();
        DoubleMatrix1D m = ma.viewColumn(0); 
        m.assign(ma.viewColumn(1), new DoubleDoubleFunction() 
            {
                private double ox = en.get(0, 0);
                private double oy = en.get(0, 1);
                
                public double apply(double x, double y)
                {
                    return Math.sqrt((x - ox) * (x - ox) + (y - oy) * (y - oy));
                }
            }
        );
        DoubleMatrix1D a = ma.viewColumn(1); 
        a.assign(en.viewColumn(0), new DoubleDoubleFunction()
            {
                private double ox = en.get(0, 0);
                private double oy = en.get(0, 1);
                
                public double apply(double y, double x)
                {
                    return Math.atan2(y - oy, x - ox);
                }
            }
        );  
        return ma;
    }
    
	/** Gets time/magnitude/azimuth data.
	 * @@return the Nx3 data
	 */
    public DoubleMatrix2D getVelocityData()
    {
        DoubleMatrix2D ma = getVelocityDataWithoutTime();
        return DoubleFactory2D.dense.compose(new DoubleMatrix2D[][] {{tenData.viewPart(0, 0, tenData.rows(), 1), ma}});
    }
    
	/** Gets a rotation matrix for the specified azimuth.
	 * @@param theta the azimuth in degrees
	 * @@return the 2x2 rotation matrix
	 */
    private DoubleMatrix2D getRotationMatrix(double theta)
    {
        DoubleMatrix2D rm = DoubleFactory2D.dense.make(2, 2);
        double tr = Math.toRadians(theta);
        rm.setQuick(0, 0, Math.cos(tr));
        rm.setQuick(0, 1, Math.sin(tr));
        rm.setQuick(1, 0, -Math.sin(tr));
        rm.setQuick(1, 1, Math.cos(tr));
        return rm;
    }
    
	/** Gets the optimal azimuth for placing the most tilt in the radial 
	 * direction. 
	 * @@return the optimal azimuth in degrees
	 */
    public double getOptimalAzimuth()
    {
		if (tenData.rows() <= 1)
			return 0;
	
        DoubleMatrix2D G = DoubleFactory2D.dense.compose(new DoubleMatrix2D[][] {{
                tenData.viewPart(0, 0, tenData.rows(), 1),
                DoubleFactory2D.dense.make(tenData.rows(), 1, 1.0)
            }});
        DoubleMatrix2D Ginv = Algebra.DEFAULT.mult(Algebra.DEFAULT.inverse(Algebra.DEFAULT.mult(G.viewDice(), G)), G.viewDice());
        double minR = Double.MAX_VALUE, minTheta = 0;
        
        for (double theta = 0.0; theta <= 360.0; theta += 1.0)
        {
            DoubleMatrix2D rm = getRotationMatrix(theta);
            DoubleMatrix2D D = Algebra.DEFAULT.mult(rm, tenData.viewPart(0, 1, tenData.rows(), 2).viewDice());
            DoubleMatrix2D d = D.viewPart(0, 0, 1, D.columns()).viewDice();
            DoubleMatrix2D m = Algebra.DEFAULT.mult(Ginv, d);
            DoubleMatrix2D r = d.assign(Algebra.DEFAULT.mult(G, m), cern.jet.math.Functions.minus);
            r = Algebra.DEFAULT.mult(r.viewDice(), r);
            double val = r.get(0, 0);
            if (val < minR)
            {
                minR = val;
                minTheta = theta;
            }
        }
        minTheta = minTheta % 360.0;
        minTheta = 360 - minTheta;
        return minTheta;
    }
}