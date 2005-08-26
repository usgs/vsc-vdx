package gov.usgs.vdx.data.gps;

import java.nio.ByteBuffer;

import gov.usgs.util.Util;
import gov.usgs.vdx.data.BinaryDataSet;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class DataPoint implements BinaryDataSet
{
	public double t;
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
	
	public String toString()
	{
		return t + "/" + Util.j2KToDate(t) + ": " + x + " " + y + " " + z;
	}

	public ByteBuffer toBinary()
	{
		ByteBuffer bb = ByteBuffer.allocate(11 * 8);
		bb.putDouble(t);
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

	public void fromBinary(ByteBuffer bb)
	{
		t = bb.getDouble();
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
