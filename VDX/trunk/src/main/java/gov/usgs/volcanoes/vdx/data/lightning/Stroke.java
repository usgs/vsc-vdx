package gov.usgs.volcanoes.vdx.data.lightning;

import gov.usgs.proj.Projection;

import java.awt.geom.Point2D;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;

/**
 * A class that contains lightning strike information. 
 * 
 * Modeled after hypo.Hypocenter
 *
 * @author Tom Parker
 */
public class Stroke {
	public double j2ksec;
	public int rid;
	public double lat;
	public double lon;
	public int stationsDetected;
	public double residual;

	public Stroke(double j2ksec, int rid, double lat, double lon, int stationsDetected, double residual) {
		this.j2ksec = j2ksec;
		this.rid = rid;
		this.lat = lat;
		this.lon = lon;
		this.stationsDetected = stationsDetected;
		this.residual = residual;
	}

	/**
	 * Apply projection to this strike
	 *
	 * @param proj
	 *            projection to be applied
	 */
	public void project(Projection proj) {
		Point2D.Double pt = new Point2D.Double(this.lon, this.lat);
		pt = proj.forward(pt);
		this.lon = pt.x;
		this.lat = pt.y;
	}

	/**
	 * Gets the raw data header line.
	 * 
	 * @return the header line
	 */
	public static String getHeaderLine() {
		return "j2ksec,lat,lon,stationsDetected,residual";
	}

	/**
	 * Converts the data to a comma-delimited String.
	 * 
	 * @return the comma-delimited String
	 */
	public String toString() {
		return String.format("%f,%f,%f,%i,%f", j2ksec, lat, lon, stationsDetected, residual);
	}

	/**
	 * Push data about this strike in the byte buffer
	 *
	 * @param buffer
	 *            ByteBuffer to push
	 */
	public void insertIntoByteBuffer(ByteBuffer buffer) {
		buffer.putDouble(j2ksec);
		buffer.putInt(rid);
		buffer.putDouble(lat);
		buffer.putDouble(lon);
		buffer.putInt(stationsDetected);
		buffer.putDouble(residual);
	}

	/**
	 * Outputs an array of Earthquakes as raw data.
	 * 
	 * @param fn
	 *            the output filename
	 * @param extra
	 *            any extra information
	 * @param data
	 *            the earthquakes
	 */
	public static void outputRawData(String fn, String extra, Stroke[] data) {
		try {
			PrintWriter out = new PrintWriter(new FileWriter(fn, true));
			out.print("Lightning strikes");
			if (extra != null)
				out.println(" [" + extra + "]");
			else
				out.println();
			getHeaderLine();
			for (int i = 0; i < data.length; i++)
				out.println(data[i].toString());
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
