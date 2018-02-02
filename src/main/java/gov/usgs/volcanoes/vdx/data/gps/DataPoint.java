package gov.usgs.volcanoes.vdx.data.gps;

import gov.usgs.volcanoes.core.data.BinaryDataSet;

import java.nio.ByteBuffer;

/**
 * GPS data point.
 *
 * @author Dan Cervelli
 */
public class DataPoint implements BinaryDataSet {

  public double timeVal;
  public double rankVal;
  public double xcoord;
  public double ycoord;
  public double zcoord;
  public double sxx;
  public double syy;
  public double szz;
  public double sxy;
  public double sxz;
  public double syz;
  public double len = Double.NaN;

  /**
   * Get string data point representation.
   *
   * @return string data point representation
   */
  public String toString() {
    return String.format(
        "t:%.8f r:%.8f x:%.8f y:%.8f z:%.8f sx:%.8f sy:%.8f sz:%.8f sxy:%.8f sxz:%.8f syz:%.8f",
        timeVal, rankVal, xcoord, ycoord, zcoord, sxx, syy, szz, sxy, sxz, syz);
  }

  /**
   * Get binary data point representation.
   *
   * @return ByteBuffer of this DataPoint
   */
  public ByteBuffer toBinary() {
    ByteBuffer bb = ByteBuffer.allocate(12 * 8);
    bb.putDouble(timeVal);
    bb.putDouble(rankVal);
    bb.putDouble(xcoord);
    bb.putDouble(ycoord);
    bb.putDouble(zcoord);
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
   * Initialize data point from binary representation.
   *
   * @param bb ByteBuffer of initial data
   */
  public void fromBinary(ByteBuffer bb) {
    timeVal = bb.getDouble();
    rankVal = bb.getDouble();
    xcoord = bb.getDouble();
    ycoord = bb.getDouble();
    zcoord = bb.getDouble();
    sxx = bb.getDouble();
    syy = bb.getDouble();
    szz = bb.getDouble();
    sxy = bb.getDouble();
    sxz = bb.getDouble();
    syz = bb.getDouble();
    len = bb.getDouble();
  }
}
