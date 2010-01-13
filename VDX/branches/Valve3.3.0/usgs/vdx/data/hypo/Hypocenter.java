package gov.usgs.vdx.data.hypo;

import gov.usgs.proj.Projection;
import gov.usgs.util.Util;

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
	public static final int T = 0;
	/** Flag for time of earthquake.
	 */
    public static final int TIME = 0;
	/** Flag for longitude of the earthquake.
	 */
    public static final int LON = 1;
    /** Flag for projected X coordinate of the earthquake.
	 */
    public static final int X = 1;
	/** Flag for latitude of the earthquake.
	 */
    public static final int LAT = 2;
    /** Flag for projected Y coordinate of the earthquake.
	 */
    public static final int Y = 2;
	/** Flag for depth of the earthquake.
	 */
    public static final int DEPTH = 3;
	/** Flag for magnitude of the earthquake.
	 */
    public static final int MAG = 4;
    
    private double[] data;

	/** Empty constructor.
	 */
    public Hypocenter() {}

	/** Constructor that sets data.
	 * @param d the data
	 */
    public Hypocenter(double[] d)
    {
        setData(d);
    }
    
	/** Sets the data.
	 * @param d data
	 */
    public void setData(double[] d)
    {
        data = d;
    }
    
	/** Gets a piece of the data.
	 * @param i the data index
	 * @return the data
	 */
    public double get(int i)
    {
        return data[i];
    }
    
	/** Sets a piece of the data.
	 * @param i the data index
	 * @param d the value
	 */
	public void set(int i, double d)
	{
		data[i] = d;
	}
	
	/** Shortcut for get(TIME).
	 * @return the time
	 */
    public double getTime()
    {
        return data[TIME];
    }
    
    public int getEID()
    {
    	return 0;
    }
    
    public int getRID()
    {
    	return 0;
    }
    
	/** Adds to the time of this earthquake.
	 * @param d the time to add (j2ksec)
	 */
    public void addTime(double d)
    {
        data[TIME] += d;
    }
    
	/** Shortcut for get(LON).
	 * @return the longitude
	 */
    public double getLon()
    {
        return data[LON];
    }
    
	/** Shortcut for set(LON, d).
	 * @param d the longitude
	 */
    public void setLon(double d)
    {
        data[LON] = d;
    }
    
	/** Shortcut for get(LAT).
	 * @return the latitude
	 */
    public double getLat()
    {
        return data[LAT];
    }
    
	/** Shortcut for set(LAT, d).
	 * @param d the latitude
	 */
    public void setLat(double d)
    {
        data[LAT] = d;
    }
    
	/** Shortcut for get(DEPTH).
	 * @return the depth
	 */
    public double getDepth()
    {
        return data[DEPTH];
    }
	
    /** Shortcut for set(DEPTH, d)
	 * @param d the depth
	 */
    public void setDepth(double d)
    {
        data[DEPTH] = d;
    }
    
    /** Longhand for getMag().
	 * @return the magnitude
	 */
    public double getMagnitude()
    {
        return getMag();
    }
    
	/** Shortcut for get(MAG).
	 * @return the magnitude
	 */
    public double getMag()
    {
        return data[MAG];
    }
    
	/** Shortcut for set(MAG, d).
	 * @param d the magnitude
	 */
    public void setMag(double d)
    {
        data[MAG] = d;
    }
    
    public Integer getNPhases()
    {
    	return null;
    }
    
    public Integer getAzgap()
    {
    	return null;
    }
    
    public double getDmin()
    {
    	return Double.NaN;
    }
    
    public double getRms()
    {
    	return Double.NaN;
    }
    
    public Integer getNstimes()
    {
    	return null;
    }
	
    public double getHerr()
    {
    	return Double.NaN;
    }
    
    public double getVerr()
    {
    	return Double.NaN;
    }
    
    public String getMagtype()
    {
    	return null;
    }
    
    public String getRemark()
    {
    	return null;
    }
    
    public void project(Projection proj)
    {
    	Point2D.Double pt = new Point2D.Double(data[LON], data[LAT]);
		pt = proj.forward(pt);
		data[X] = pt.x;
		data[Y] = pt.y;
    }
    
	/** Gets the raw data header line.
	 * @return the header line
	 */
    public static String getHeaderLine()
    {
        return "j2ksec,lon,lat,depth,mag";
    }
    
	/** Converts the data to a comma-delimited String.
	 * @return the comma-delimited String
	 */
    public String toString()
    {
        return data[0] + "," + Util.formatDate(Util.j2KToDate(data[0])) + "," + data[1] + "," + data[2] + "," + data[3] + "," + data[4];
    }
	
    public void insertIntoByteBuffer(ByteBuffer buffer)
    {
    	for (int i = 0; i < data.length; i++)
    		buffer.putDouble(data[i]);
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
			out.println("j2ksec,date,lon,lat,depth,mag");
			for (int i = 0; i < data.length; i++)
				out.println(data[i]);
			out.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
