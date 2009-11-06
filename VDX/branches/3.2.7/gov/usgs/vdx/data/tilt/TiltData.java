package gov.usgs.vdx.data.tilt;

import gov.usgs.vdx.data.GenericDataMatrix;

import java.util.List;

import cern.colt.function.DoubleDoubleFunction;
import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.linalg.Algebra;

/**
 * GenericDataMatrix with 3 columns: time/east/north
 * to store tilt data
 *
 * @author Dan Cervelli
 */
public class TiltData extends GenericDataMatrix
{
	/**
	 * Default constructor
	 */
	public TiltData()
	{
		super();
	}
	
	/**
	 * Constructor
	 * @param pts list of raws, each of them is double[3]
	 */
	public TiltData(List<double[]> pts)
	{
		super(pts);
	}
	
	/**
	 * Set predefined column names: time/east/north
	 */
	public void setColumnNames()
	{
		columnMap.put("time", 0);
		columnMap.put("east", 1);
		columnMap.put("north", 2);
	}
	
	/** Get the time/east/north data for a specific row.
	 * @param row the row number
	 * @return t/e/n for that row
	 */
    public double[] getTEN(int row)
    {
        return new double[] {data.get(row, 0), data.get(row, 1), data.get(row, 2)};
    }

	/** Gets time/east/north/radial/tangential/magnitude/azimuth data using
	 * the specified azimuth.
	 * @param theta azimuth in degrees
	 * @return the Nx7 data matrix
	 */
    public DoubleMatrix2D getAllData(double theta)
    {
        return DoubleFactory2D.dense.compose(new DoubleMatrix2D[][] 
                {{data,
                  getRotatedDataWithoutTime(theta),
                  getVelocityDataWithoutTime()}});
    }
    
	/** Gets radial/tangential data using the specified azimuth.
	 * @param theta the azimuth in degrees
	 * @return the Nx2 data matrix
	 */
    public DoubleMatrix2D getRotatedDataWithoutTime(double theta)
    {
        DoubleMatrix2D en = data.viewPart(0, 1, data.rows(), 2);
        return Algebra.DEFAULT.mult(en, getRotationMatrix(theta));
    }
	
    /** Gets time/radial/tangential data using the specified azimuth.
	 * @param theta the azimuth in degrees
	 * @return the Nx3 data matrix
	 */
    public DoubleMatrix2D getRotatedData(double theta)
    {
        DoubleMatrix2D rt = getRotatedDataWithoutTime(theta);
        return DoubleFactory2D.dense.compose(new DoubleMatrix2D[][] {{data.viewPart(0, 0, data.rows(), 1), rt}});
    }
    
	/** Gets magnitude/azimuth data.
	 * @return the Nx2 data matrix
	 */
    public DoubleMatrix2D getVelocityDataWithoutTime()
    {
        final DoubleMatrix2D en = data.viewPart(0, 1, data.rows(), 2);
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
	 * @return the Nx3 data
	 */
    public DoubleMatrix2D getVelocityData()
    {
        DoubleMatrix2D ma = getVelocityDataWithoutTime();
        return DoubleFactory2D.dense.compose(new DoubleMatrix2D[][] {{data.viewPart(0, 0, data.rows(), 1), ma}});
    }
    
	/** Gets a rotation matrix for the specified azimuth.
	 * @param theta the azimuth in degrees
	 * @return the 2x2 rotation matrix
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
	 * @return the optimal azimuth in degrees
	 */
    public double getOptimalAzimuth()
    {
		if (data.rows() <= 1)
			return 0;
	
        DoubleMatrix2D G = DoubleFactory2D.dense.compose(new DoubleMatrix2D[][] {{
                data.viewPart(0, 0, data.rows(), 1),
                DoubleFactory2D.dense.make(data.rows(), 1, 1.0)
            }});
        DoubleMatrix2D Ginv = Algebra.DEFAULT.mult(Algebra.DEFAULT.inverse(Algebra.DEFAULT.mult(G.viewDice(), G)), G.viewDice());
        double minR = Double.MAX_VALUE, minTheta = 0;
        
        for (double theta = 0.0; theta <= 360.0; theta += 1.0)
        {
            DoubleMatrix2D rm = getRotationMatrix(theta);
            DoubleMatrix2D D = Algebra.DEFAULT.mult(rm, data.viewPart(0, 1, data.rows(), 2).viewDice());
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
