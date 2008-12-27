package gov.usgs.vdx.data.other;

import gov.usgs.vdx.data.GenericDataMatrix;

import java.nio.ByteBuffer;
import java.util.List;

import cern.colt.matrix.DoubleMatrix2D;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class RainfallData extends GenericDataMatrix
{
	/** Generic empty constructor
	 */
	public RainfallData()
	{
		columnMap.put("time", 0);
		columnMap.put("rainfall", 0);
	}

	/**
	 * Create an RSAMData from a byte buffer.  This first 4 bytes specify an
	 * integer number of rows followed by rows*16 bytes, 2 doubles: j2ksec and 
	 * RSAM.
	 * 
	 * @param bb the byte buffer
	 */
	public RainfallData(ByteBuffer bb)
	{
		super(bb);
	}

	public RainfallData(List<double[]> list)
	{
		super(list);
	}
	
	public void setData(DoubleMatrix2D d)
	{
		data = d;
		double total = 0;
		double r;
		double last = d.getQuick(0, 1);
		
		for (int i = 1; i < d.rows(); i++) {
			r = d.getQuick(i, 1);
			if (r < last) {
				last = 0;
			}
			total += (r - last);
			d.setQuick(i, 1, total);
			last = r;
		}
	}

	/** Gets the RSAM column (column 2) of the data. 
	 * @return the data column
	 */
	public DoubleMatrix2D getRainfall()
	{
		return data.viewPart(0, 1, rows(), 1);
	}

}
