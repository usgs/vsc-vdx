package gov.usgs.vdx.data.rsam;

import gov.usgs.vdx.data.GenericDataMatrix;

import java.nio.ByteBuffer;
import java.util.List;

import cern.colt.matrix.DoubleMatrix2D;

/**
 * A class that deals with RSAM data.  The data are stored in a 2-D matrix, the
 * first column is the time (j2ksec), the second is the data.
 *
 * $Log: not supported by cvs2svn $
 * Revision 1.2  2005/04/07 22:54:04  cervelli
 * Added ByteBuffer functions and List based constructor.
 *
 * Revision 1.1  2004/10/12 18:21:25  cvs
 * Initial commit.
 *
 * @author Dan Cervelli
 */
public class RSAMData extends GenericDataMatrix
{
	/** Generic empty constructor
	 */
	public RSAMData()
	{
		columnMap.put("time", 0);
		columnMap.put("rsam", 0);
	}

	/**
	 * Create an RSAMData from a byte buffer.  This first 4 bytes specify an
	 * integer number of rows followed by rows*16 bytes, 2 doubles: j2ksec and 
	 * RSAM.
	 * 
	 * @param bb the byte buffer
	 */
	public RSAMData(ByteBuffer bb)
	{
		super(bb);
	}

	public RSAMData(List<double[]> list)
	{
		super(list);
	}

	/** Gets the RSAM column (column 2) of the data. 
	 * @return the data column
	 */
	public DoubleMatrix2D getRSAM()
	{
		return data.viewPart(0, 1, rows(), 1);
	}
}