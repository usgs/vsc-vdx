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
public class VoltageData extends GenericDataMatrix
{
	/** Generic empty constructor
	 */
	public VoltageData()
	{
		columnMap.put("time", 0);
		columnMap.put("voltage", 0);
	}

	/**
	 * Create an RSAMData from a byte buffer.  This first 4 bytes specify an
	 * integer number of rows followed by rows*16 bytes, 2 doubles: j2ksec and 
	 * RSAM.
	 * 
	 * @param bb the byte buffer
	 */
	public VoltageData(ByteBuffer bb)
	{
		super(bb);
	}

	public VoltageData(List<double[]> list)
	{
		super(list);
	}

	/** Gets the RSAM column (column 2) of the data. 
	 * @return the data column
	 */
	public DoubleMatrix2D getVoltage()
	{
		return data.viewPart(0, 1, rows(), 1);
	}

}
