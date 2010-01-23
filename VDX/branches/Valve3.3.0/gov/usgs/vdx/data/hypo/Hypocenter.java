package gov.usgs.vdx.data.hypo;

import gov.usgs.proj.Projection;

import java.awt.geom.Point2D;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;

/**
 * A class that contains standard earthquake hypocenter information.  The data
 * are stored in an array with public index tags for retrieval (example: Earthquake.get(TIME)).
 *
 * @author  Dan Cervelli
 * @version 2.02
 */
public class Hypocenter
{    
    public double j2ksec;
    public String eid;
    public Integer rid;
    public double lat;
    public double lon;
    public double depth;
    public double prefmag;
    public double ampmag;
    public double codamag;
    public Integer nphases;
    public Integer azgap;
    public double dmin;
    public double rms;
    public Integer nstimes;
    public double herr;
    public double verr;
    public String magtype;
    public String rmk;

	/** Constructor that sets data.
	 * @param d the data
	 */
    public Hypocenter(double j2ksec, Integer rid, double lat, double lon, double depth, double prefmag) {
        this(j2ksec, (String)null, rid, lat, lon, depth, prefmag, Double.NaN, Double.NaN,
        		(Integer)null, (Integer)null, Double.NaN, Double.NaN, (Integer)null, Double.NaN, Double.NaN, (String)null, (String)null);
    }
    
    public Hypocenter(double j2ksec, String eid, Integer rid, double lat, double lon, double depth, double prefmag, double ampmag, double codamag, 
    		Integer nphases, Integer azgap, double dmin, double rms, Integer nstimes, double herr, double verr, String magtype, String rmk) {
    	this.j2ksec		= j2ksec;
    	this.eid		= eid;
    	this.rid		= rid;
    	this.lat		= lat;
    	this.lon		= lon;
    	this.depth		= depth;
    	this.prefmag	= prefmag;
    	this.ampmag		= ampmag;
    	this.codamag	= codamag;
    	this.nphases	= nphases;
    	this.azgap		= azgap;
    	this.dmin		= dmin;
    	this.rms		= rms;
    	this.nstimes	= nstimes;
    	this.herr		= herr;
    	this.verr		= verr;
    	this.magtype	= magtype;
    	this.rmk		= rmk;
    }
    
    public void project(Projection proj)
    {
    	Point2D.Double pt = new Point2D.Double(this.lon, this.lat);
		pt = proj.forward(pt);
		this.lon = pt.x;
		this.lat = pt.y;
    }
    
	/** Gets the raw data header line.
	 * @return the header line
	 */
    public static String getHeaderLine()
    {
        return "j2ksec,lat,lon,depth,mag";
    }
    
	/** Converts the data to a comma-delimited String.
	 * @return the comma-delimited String
	 */
    public String toString()
    {
        return this.j2ksec + "," + this.lat + "," + this.lon + "," + this.depth + "," + this.prefmag;
    }
	
    public void insertIntoByteBuffer(ByteBuffer buffer)
    {
    	buffer.putDouble(j2ksec);
    	buffer.putInt(rid);
    	buffer.putDouble(lat);
    	buffer.putDouble(lon);
    	buffer.putDouble(depth);
    	buffer.putDouble(prefmag);
    }
    
	/** Outputs an array of Earthquakes as raw data.
	 * @param fn the output filename
	 * @param extra any extra information
	 * @param data the earthquakes
	 */
	public static void outputRawData(String fn, String extra, Hypocenter[] data)
	{
		try
		{
			PrintWriter out = new PrintWriter(new FileWriter(fn, true));
			out.print("Earthquake hypocenters");
			if (extra != null)
				out.println(" [" + extra + "]");
			else
				out.println();
			getHeaderLine();
			for (int i = 0; i < data.length; i++)
				out.println(data[i].toString());
			out.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
