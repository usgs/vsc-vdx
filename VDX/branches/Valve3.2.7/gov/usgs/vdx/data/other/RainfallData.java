package gov.usgs.vdx.data.other;

import gov.usgs.vdx.data.GenericDataMatrix;

import java.nio.ByteBuffer;
import java.util.List;

import cern.colt.matrix.DoubleMatrix2D;

/**
 * 
 * @author Loren Antolik
 */
public class RainfallData extends GenericDataMatrix {
	
	/** Generic empty constructor
	 */
	public RainfallData() {
		columnMap.put("time", 0);
		columnMap.put("rainfall", 0);
	}

	/**
	 * Create a RainfallData from a byte buffer.  This first 4 bytes specify an
	 * integer number of rows followed by rows*16 bytes, 2 doubles: j2ksec and 
	 * Rainfall.
	 * 
	 * @param bb the byte buffer
	 */
	public RainfallData(ByteBuffer bb) {
		super(bb);
	}

	public RainfallData(List<double[]> list) {
		super(list);
	}
	
	/**
	 * Converts the data acquired from the database to a time series format.
	 * The data becomes a cumulative value, and increases only when the values from
	 * the database increase.  Consecutive identical values, or rezeroing of the data
	 * indicate no new rain, or a tipping of the bucket
	 * 
	 * @param d the matrix of time and rainfall data
	 */
	public void setData(DoubleMatrix2D d) {
		
		// initialize variables and set default values
		data = d;
		double total = 0;
		double r;
		double last = d.getQuick(0, 1);
		
		// set the initial amount of rainfall to be zero for this time period
		d.setQuick(0, 1, 0);
		
		// iterate through all subsequent rows and assign a rainfall amount if the 
		// data increases.  Keep the total if the rainfall is less than the previous reading
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

	/** Gets the Rainfall column (column 2) of the data. 
	 * @return the data column
	 */
	public DoubleMatrix2D getRainfall() {
		return data.viewPart(0, 1, rows(), 1);
	}

}
