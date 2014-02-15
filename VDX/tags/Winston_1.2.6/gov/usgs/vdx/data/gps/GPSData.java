package gov.usgs.vdx.data.gps;

import gov.usgs.util.CodeTimer;
import gov.usgs.util.Log;
import gov.usgs.vdx.data.BinaryDataSet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.linalg.Algebra;

/**
 * A class that deals with GPS data.  The data are stored in four matrices:
 * an Nx1 time matrix, an Nx1 rank matrix, a (3*N)x1 position matrix, 
 * and a sparse (3*N)x(3*N) covariance matrix, where N is the number of observations.
 *
 * @author Dan Cervelli
 */
public class GPSData implements BinaryDataSet
{
	protected final static Logger logger = Log.getLogger("gov.usgs.vdx.data.gps.GPSData"); 
	private static final DoubleFactory2D DENSE	= DoubleFactory2D.dense;
	private static final DoubleFactory2D SPARSE	= DoubleFactory2D.sparse;
	private static final DoubleMatrix2D I3X3	= DENSE.identity(3);
	private static final DoubleMatrix2D ZERO3X3	= DENSE.make(3, 3);

	private DoubleMatrix2D tData;
	private DoubleMatrix2D rData;
	private DoubleMatrix2D xyzData;
	private DoubleMatrix2D covData;
	private DoubleMatrix2D lenData;
	
	/** Generic empty constructor.
	 */
	public GPSData() {}
	
	/**
	 * Constructor
	 * @param pts list of DataPoints
	 */
	public GPSData(List pts)
	{
		setToList(pts);
	}
	
	/**
	 * Initialize internal matrices from list of DataPoints
	 * @param pts list of DataPoints
	 */
	public void setToList(List pts)
	{
		int rows = pts.size();
		DataPoint origin	= (DataPoint)pts.get(0);
		boolean hasLen		= !Double.isNaN(origin.len);
		tData				= DENSE.make(rows, 1);
		rData				= DENSE.make(rows, 1);
		xyzData				= DENSE.make(rows * 3, 1);
		covData				= SPARSE.make(rows * 3, rows * 3);
		lenData				= DENSE.make(rows, 1);
		for (int i = 0; i < rows; i++)
		{
			DataPoint dp = (DataPoint)pts.get(i);
			// set times
			tData.setQuick(i, 0, dp.t);
			
			// set ranks
			rData.setQuick(i, 0, dp.r);
			
			// set xyz
			xyzData.setQuick(i * 3, 0, dp.x);
			xyzData.setQuick(i * 3 + 1, 0, dp.y);
			xyzData.setQuick(i * 3 + 2, 0, dp.z);

			// set sigmas
			covData.setQuick(i * 3, i * 3, dp.sxx);
			covData.setQuick(i * 3, i * 3 + 1, dp.sxy);
			covData.setQuick(i * 3, i * 3 + 2, dp.sxz);
			
			covData.setQuick(i * 3 + 1, i * 3, dp.sxy);
			covData.setQuick(i * 3 + 1, i * 3 + 1, dp.syy);
			covData.setQuick(i * 3 + 1, i * 3 + 2, dp.syz);
			
			covData.setQuick(i * 3 + 2, i * 3, dp.sxz);
			covData.setQuick(i * 3 + 2, i * 3 + 1, dp.syz);
			covData.setQuick(i * 3 + 2, i * 3 + 2, dp.szz);
			
			//set length
			if (hasLen)
				lenData.setQuick(i, 0, dp.len);
			else
			{
				double dx = dp.x - origin.x;
				double dy = dp.y - origin.y;
				double dz = dp.z - origin.z;
				lenData.setQuick(i, 0, Math.sqrt(dx * dx + dy * dy + dz * dz));
			}
		}
	}
	
	/**
	 * Get binary GPSData representation
	 * @return ByteBuffer of binary GPSData
	 */
	public ByteBuffer toBinary()
	{
		int rows = observations();
		ByteBuffer bb = ByteBuffer.allocate(4 + rows * 12 * 8);
		bb.putInt(rows);
		for (int i = 0; i < rows; i++)
		{
			bb.putDouble(tData.getQuick(i, 0)); // t
			
			bb.putDouble(rData.getQuick(i, 0)); // r
			
			bb.putDouble(xyzData.getQuick(i * 3, 0)); // x
			bb.putDouble(xyzData.getQuick(i * 3 + 1, 0)); // y
			bb.putDouble(xyzData.getQuick(i * 3 + 2, 0)); // z
			
			bb.putDouble(covData.getQuick(i * 3, i * 3)); // sxx
			bb.putDouble(covData.getQuick(i * 3 + 1, i * 3 + 1)); // syy
			bb.putDouble(covData.getQuick(i * 3 + 2, i * 3 + 2)); // szz
			
			bb.putDouble(covData.getQuick(i * 3 + 1, i * 3)); // sxy
			bb.putDouble(covData.getQuick(i * 3 + 2, i * 3)); // sxz
			bb.putDouble(covData.getQuick(i * 3 + 2, i * 3 + 1)); //syz
			
			bb.putDouble(lenData.getQuick(i, 0)); // len
		}
		return bb;
	}

	/**
	 * Initialize GPSData from binary representation
	 * @param bb ByteBuffer of GPSData
	 */
	public void fromBinary(ByteBuffer bb)
	{
		int rows = bb.getInt();
		tData	= DENSE.make(rows, 1);
		rData	= DENSE.make(rows, 1);
		xyzData	= DENSE.make(rows * 3, 1);
		covData	= SPARSE.make(rows * 3, rows * 3);
		lenData	= DENSE.make(rows, 1);
		DataPoint dp = new DataPoint();
		for (int i = 0; i < rows; i++)
		{
			dp.fromBinary(bb);
			// set times
			tData.setQuick(i, 0, dp.t);
			
			// set ranks
			rData.setQuick(i, 0, dp.r);
			
			// set xyz
			xyzData.setQuick(i * 3, 0, dp.x);
			xyzData.setQuick(i * 3 + 1, 0, dp.y);
			xyzData.setQuick(i * 3 + 2, 0, dp.z);

			// set sigmas
			covData.setQuick(i * 3, i * 3, dp.sxx);
			covData.setQuick(i * 3, i * 3 + 1, dp.sxy);
			covData.setQuick(i * 3, i * 3 + 2, dp.sxz);
			
			covData.setQuick(i * 3 + 1, i * 3, dp.sxy);
			covData.setQuick(i * 3 + 1, i * 3 + 1, dp.syy);
			covData.setQuick(i * 3 + 1, i * 3 + 2, dp.syz);
			
			covData.setQuick(i * 3 + 2, i * 3, dp.sxz);
			covData.setQuick(i * 3 + 2, i * 3 + 1, dp.syz);
			covData.setQuick(i * 3 + 2, i * 3 + 2, dp.szz);
		}
	}
	
	/** Sets the data matrices.
	 * @param t the time matrix
	 * @param xyz the position matrix
	 * @param cov the covariance matrix
	 */
	public void setData(DoubleMatrix2D t, DoubleMatrix2D r, DoubleMatrix2D xyz, DoubleMatrix2D cov)
	{
		tData	= t;
		rData	= r;
		xyzData	= xyz;
		covData	= cov;
	}
	
	/** Gets the number of observations.
	 * @return the number of observations
	 */
	public int observations()
	{
		return tData.rows();
	}
	
	/** Adds a value to the time column (for time zone management).
	 * @param adj the time adjustment
	 */
	public void adjustTime(double adj)
	{
		for (int i = 0; i < tData.rows(); i++)
			tData.setQuick(i, 0, tData.getQuick(i, 0) + adj);
	}
	
	/** Gets the time matrix.
	 * @return the time matrix
	 */
	public DoubleMatrix2D getTimes()
	{
		return tData;
	}
	
	/** Gets the rank matrix.
	 * @return the rank matrix
	 */
	public DoubleMatrix2D getRanks()
	{
		return rData;
	}
	
	/** Gets the position matrix.
	 * @return the position matrix
	 */
	public DoubleMatrix2D getXYZ()
	{
		return xyzData;
	}
	
	/** Gets the covariance matrix.
	 * @return the covariance matrix
	 */
	public DoubleMatrix2D getCovariance()
	{
		return covData;
	}
	
	/** Gets the origin (the first position).
	 * @return xyz of the first position
	 */
	public double[] getOrigin()
	{
		return new double[] {xyzData.get(0, 0), xyzData.get(1, 0), xyzData.get(2, 0)};
	}
	
	/** Subtracts position and adds covariance of specified baseline data at
	 * subset of times common to both this data and the baseline data. Data
	 * need to be sorted in time ascending order.
	 * 
	 * @param baseline the baseline data
	 */
	public void applyBaseline(GPSData baseline)
	{
		CodeTimer ct = new CodeTimer("applyBaseline");
		int si = 0; 
		int bi = 0;
		List<DataPoint> data = new ArrayList<DataPoint>(observations());
		boolean done = false;
		while (!done)
		{
			double st = tData.getQuick(si, 0);
			double bt = baseline.tData.getQuick(bi, 0);
			if (Math.abs(st - bt) < 0.001)
			{
				DataPoint dp = new DataPoint();
				dp.t = st;
				dp.x = xyzData.getQuick(si * 3, 0) - baseline.xyzData.getQuick(bi * 3, 0);
				dp.y = xyzData.getQuick(si * 3 + 1, 0) - baseline.xyzData.getQuick(bi * 3 + 1, 0);
				dp.z = xyzData.getQuick(si * 3 + 2, 0) - baseline.xyzData.getQuick(bi * 3 + 2, 0);
				
				dp.sxx = covData.getQuick(si * 3, si * 3) + baseline.covData.getQuick(bi * 3, bi * 3);
				dp.syy = covData.getQuick(si * 3 + 1, si * 3 + 1) + baseline.covData.getQuick(bi * 3 + 1, bi * 3 + 1);
				dp.szz = covData.getQuick(si * 3 + 2, si * 3 + 2) + baseline.covData.getQuick(bi * 3 + 2, bi * 3 + 2);
				
				dp.sxy = covData.getQuick(si * 3, si * 3 + 1) + baseline.covData.getQuick(bi * 3, bi * 3 + 1);
				dp.sxz = covData.getQuick(si * 3, si * 3 + 2) + baseline.covData.getQuick(bi * 3, bi * 3 + 2);
				dp.syz = covData.getQuick(si * 3 + 1, si * 3 + 2) + baseline.covData.getQuick(bi * 3 + 1, bi * 3 + 2);
				
				dp.len = Math.sqrt(dp.x * dp.x + dp.y * dp.y + dp.z * dp.z);
				data.add(dp);
				si++;
				bi++;
			}
			else
			{
				if (st < bt)
				{
					do
					{
						si++;
					} while (si < tData.rows() && tData.getQuick(si, 0) < baseline.tData.getQuick(bi, 0));
					if (si == tData.rows())
						done = true;
				}
				else
				{
					do
					{
						bi++;
					} while (bi < baseline.tData.rows() && baseline.tData.getQuick(bi, 0) < tData.getQuick(si, 0));
					if (bi == baseline.tData.rows())
						done = true;
				}
			}
			if (si == tData.rows() || bi == baseline.tData.rows())
				done = true;
		}
		
		if (data.size() > 0)
			setToList(data);
		
		ct.stopAndReport();
	}

	/** Converts the XYZ position data to east/north/up (ENU) position data 
	 * based on a specified origin.
	 * @param lon origin longitude
	 * @param lat origin latitude
	 */
	public void toENU(double lon, double lat)
	{
		// works, but slow
		/*
		DoubleMatrix2D t = GPS.createFullENUTransform(lon, lat, observations());
		xyzData = Algebra.DEFAULT.mult(t, xyzData);
		logger.info(covData.rows() + " " + covData.columns() + "\n" + t.rows() + " " + t.columns());
		CodeTimer ct = new CodeTimer("toENU just cov");
		covData = Algebra.DEFAULT.mult(Algebra.DEFAULT.mult(t, covData), t.viewDice());
		ct.stop();
		*/
		CodeTimer ct = new CodeTimer("fast toENU");
		DoubleMatrix2D t = GPS.createENUTransform(lon, lat);
		DoubleMatrix2D tt = t.viewDice();
		DoubleMatrix2D[][] xyz = new DoubleMatrix2D[observations()][1];
		DoubleMatrix2D[][] cov = new DoubleMatrix2D[observations()][observations()];
		for (int i = 0; i < observations(); i++)
		{
			xyz[i][0] = Algebra.DEFAULT.mult(t, xyzData.viewPart(i * 3, 0, 3, 1));
			cov[i][i] = Algebra.DEFAULT.mult(Algebra.DEFAULT.mult(t, covData.viewPart(i * 3, i * 3, 3, 3)), tt);
		}
		xyzData = DENSE.compose(xyz);
		covData = SPARSE.compose(cov);
		ct.stopAndReport();
//		output();
	}
	
	/** Generates position/time time-series data for plotting by Valve.  The returned 
	 * time-series data is simply an array of j2ksec, rank, e, n, u, length arrays.
	 * @param baseline baseline data or null for none
	 * @return time-series data
	 */
	public DoubleMatrix2D toTimeSeries(GPSData baseline)
	{
		double[] origin = getOrigin();
		double[] originLLH = GPS.xyz2LLH(origin[0], origin[1], origin[2]);
		if (baseline != null)
			applyBaseline(baseline);
		toENU(originLLH[0], originLLH[1]);
		
		DoubleMatrix2D enuRows = GPS.column3NToRows(xyzData);
		DoubleMatrix2D bigMatrix = DoubleFactory2D.dense.compose(new DoubleMatrix2D[][] {{tData, rData, enuRows, lenData}});
		return bigMatrix;
		// return bigMatrix.toArray();
//		output();
		// tested and good above

		/*
		double norm = Math.sqrt(xyzData.getQuick(0, 0) * xyzData.getQuick(0, 0) +
				xyzData.getQuick(1, 0) * xyzData.getQuick(1, 0) +
				xyzData.getQuick(2, 0) * xyzData.getQuick(2, 0));
		double normE = xyzData.getQuick(0, 0) / norm;
		double normN = xyzData.getQuick(1, 0) / norm;
		double normU = xyzData.getQuick(2, 0) / norm;
		DoubleMatrix2D enuRows = GPS.column3NToRows(xyzData);
		DoubleMatrix2D len = DoubleFactory2D.dense.make(enuRows.rows(), 1);
		for (int i = 0; i < len.rows(); i++)
			len.setQuick(i, 0, enuRows.getQuick(i, 0) * normE + enuRows.getQuick(i, 1) * normN + enuRows.getQuick(i, 2) * normU);
		*/
		
		/*
		DoubleMatrix2D len = DoubleFactory2D.dense.make(enuRows.rows(), 1);
		for (int i = 0; i < len.rows(); i++)
		{
			
		}
		*/
		
	}
	
	/** Creates the kernel matrix (g) for modeling velocities using weighted
	 * least squares.
	 * @return the velocity kernel matrix
	 */
	public DoubleMatrix2D createVelocityKernel()
	{
		double t0 = tData.getQuick(0, 0);
		DoubleMatrix2D[][] g = new DoubleMatrix2D[observations()][2];
		double ta;
		DoubleMatrix2D temp;
		for (int i = 0; i < observations(); i++)
		{
			ta = (tData.getQuick(i, 0) - t0) / (86400 * 365.25);
			temp = DENSE.make(3, 3);
			temp.setQuick(0, 0, ta);
			temp.setQuick(1, 1, ta);
			temp.setQuick(2, 2, ta);
			g[i][0] = temp;
			g[i][1] = I3X3;
		}
		return SPARSE.compose(g);
	}
	
	/** Creates the kernel matrix (g) for modeling a displacement at a 
	 * specified time using weighted least squares.
	 * @param dt the displacement time (j2ksec)
	 * @return the displacement kernel matrix
	 */
	public DoubleMatrix2D createDisplacementKernel(double dt)
	{
		DoubleMatrix2D[][] g = new DoubleMatrix2D[observations()][2];
		for (int i = 0; i < observations(); i++)
		{
			if (tData.getQuick(i, 0) < dt)
				g[i][0] = ZERO3X3;
			else
				g[i][0] = I3X3;
			g[i][1] = I3X3;
		}
		return SPARSE.compose(g);
	}
	
	/** Creates the kernel matrix (g) for modeling a displacement at a 
	 * specified time using weighted least squares while accounting for the
	 * pre-existing linear trend first.  
	 * @param dt the displacement time (j2ksec)
	 * @return the displacement kernel matrix
	 */
	public DoubleMatrix2D createDetrendedDisplacementKernel(double dt)
	{
		double t0 = tData.getQuick(0, 0);
		DoubleMatrix2D[][] g = new DoubleMatrix2D[observations()][3];
		double ta;
		DoubleMatrix2D temp;
		for (int i = 0; i < observations(); i++)
		{
			if (tData.getQuick(i, 0) < dt)
				g[i][0] = ZERO3X3;
			else
				g[i][0] = I3X3;
			ta = (tData.getQuick(i, 0) - t0) / (86400 * 365.25);
			temp = DENSE.make(3,3);
			temp.setQuick(0, 0, ta);
			temp.setQuick(1, 1, ta);
			temp.setQuick(2, 2, ta);
			g[i][1] = temp;
			g[i][2] = I3X3;
		}
		return SPARSE.compose(g);
	}

	/**
	 * Get first observation from internal matrices
	 * @return DataPoint of first observation
	 */
	public DataPoint getFirstObservation()
	{
		if (observations() <= 0)
			return null;
		
		DataPoint dp = new DataPoint();
		dp.t = tData.getQuick(0, 0);
		dp.r = rData.getQuick(0, 0);
		dp.x = xyzData.getQuick(0, 0);
		dp.y = xyzData.getQuick(1, 0);
		dp.z = xyzData.getQuick(2, 0);
		dp.len = lenData.getQuick(0, 0);
		
		dp.sxx = covData.getQuick(0, 0);
		dp.syy = covData.getQuick(1, 1);
		dp.szz = covData.getQuick(2, 2);
		
		dp.sxy = covData.getQuick(0, 1);
		dp.sxz = covData.getQuick(0, 2);
		dp.syz = covData.getQuick(1, 2);
		
		return dp;
	}
	
	/**
	 * Write GPSData content to log
	 */
	public void output()
	{
		for (int i = 0; i < observations(); i++)
		{
			logger.fine(new Double(tData.getQuick(i, 0)).toString());
			logger.fine(new Double(rData.getQuick(i, 0)).toString());
			logger.fine("\t" +
					xyzData.getQuick(i * 3, 0) + " " + 
					xyzData.getQuick(i * 3 + 1, 0) + " " + 
					xyzData.getQuick(i * 3 + 2, 0) + " " + 
					lenData.getQuick(i, 0));
			
			logger.fine("\t\t" +
					covData.getQuick(i * 3 + 0, i * 3) + " " + 
					covData.getQuick(i * 3 + 0, i * 3 + 1) + " " + 
					covData.getQuick(i * 3 + 0, i * 3 + 2) + " ");
			
			logger.fine("\t\t" +
					covData.getQuick(i * 3 + 1, i * 3) + " " + 
					covData.getQuick(i * 3 + 1, i * 3 + 1) + " " + 
					covData.getQuick(i * 3 + 1, i * 3 + 2) + " ");
			
			logger.fine("\t\t" +
					covData.getQuick(i * 3 + 2, i * 3) + " " + 
					covData.getQuick(i * 3 + 2, i * 3 + 1) + " " + 
					covData.getQuick(i * 3 + 2, i * 3 + 2) + " ");
		}
	}
	
	/** Outputs raw data to a text file.
	 * @param fn the output filename
	 * @param extra any extra information to include in the output file
	 */
	public void outputRawData(String fn, String extra)
	{
		/*
		if (extra == null)
			extra = "";
		else 
			extra = " [" + extra + "]";
		Util.outputData(fn, "GPS Data" + extra + "'\nj2ksec,date,x,y,z,sx,sy,sz", rs);	
		 */
	}

}
