package gov.usgs.vdx.data.gps;

import gov.usgs.vdx.data.BinaryDataSet;

import java.nio.ByteBuffer;

/**
 * GPS data point
 *
 * @author Dan Cervelli
 */
public class DataPoint implements BinaryDataSet
{
	public double t;
	public double r;
	public double x;
	public double y;
	public double z;
	public double sxx;
	public double syy;
	public double szz;
	public double sxy;
	public double sxz;
	public double syz;
	public double len = Double.NaN;
	
	/**
	 * Get string data point representation
	 * @return string data point representation
	 */
	public String toString()
	{
		return String.format("t:%.8f r:%.8f x:%.8f y:%.8f z:%.8f sx:%.8f sy:%.8f sz:%.8f sxy:%.8f sxz:%.8f syz:%.8f", 
				t, r, x, y, z, sxx, syy, szz, sxy, sxz, syz);
	}

	/**
	 * Get binary data point representation
	 * @return ByteBuffer of this DataPoint
	 */
	public ByteBuffer toBinary()
	{
		ByteBuffer bb = ByteBuffer.allocate(12 * 8);
		bb.putDouble(t);
		bb.putDouble(r);
		bb.putDouble(x);
		bb.putDouble(y);
		bb.putDouble(z);
		bb.putDouble(sxx);
		bb.putDouble(syy);
		bb.putDouble(szz);
		bb.putDouble(sxy);
		bb.putDouble(sxz);
		bb.putDouble(syz);
		bb.putDouble(len);
		return bb;
	}

	/**
	 * Initialize data point from binary representation
	 * @param bb ByteBuffer of initial data
	 */
	public void fromBinary(ByteBuffer bb)
	{
		t = bb.getDouble();
		r = bb.getDouble();
		x = bb.getDouble();
		y = bb.getDouble();
		z = bb.getDouble();
		sxx = bb.getDouble();
		syy = bb.getDouble();
		szz = bb.getDouble();
		sxy = bb.getDouble();
		sxz = bb.getDouble();
		syz = bb.getDouble();
		len = bb.getDouble();
	}
}
