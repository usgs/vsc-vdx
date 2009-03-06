package gov.usgs.vdx.data.gps;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GPS benchmark
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.2  2005/08/29 22:51:18  dcervelli
 * Added new constructor and getCode().
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class Benchmark implements Comparable<Object>
{
	private int bid;
	private String code;
	private double lon;
	private double lat;
	private double height;
	
	/**
	 * Default constructor
	 */
	public Benchmark()
	{
		bid = -1;
		code = null;
		lon = -999;
		lat = -999;
		height = -99999;
	}
	
	/**
	 * Constructor
	 * @param bm string representation, in format 'id:code:longitude:latitude:height'
	 */
	public Benchmark(String bm)
	{
		String[] parts = bm.split(":");
		bid = Integer.parseInt(parts[0]);
		code = parts[3];
		lon = Double.parseDouble(parts[1]);
		lat = Double.parseDouble(parts[2]);
		height = Double.parseDouble(parts[5]);
	}
	
	/**
	 * Constructor
	 * @param id benchmark id
	 * @param c code
	 * @param ln longitude
	 * @param lt latitude
	 * @param h height
	 */
	public Benchmark(int id, String c, double ln, double lt, double h)
	{
		bid = id;
		code = c;
		lon = ln;
		lat = lt;
		height = h;
	}
	
	/**
	 * Construct list of benchmarks from strings list 
	 * @param ss list of ':'-separated strings
	 */
	public static List<Benchmark> fromStringsToList(List<String> ss)
	{
		List<Benchmark> bms = new ArrayList<Benchmark>();
		for (String s : ss)
			bms.add(new Benchmark(s));
		
		return bms;
	}

	/**
	 * Construct map of benchmark_id-benchmark pairs from strings list 
	 * @param ss list of ':'-separated strings
	 */
	public static Map<String, Benchmark> fromStringsToMap(List<String> ss)
	{
		Map<String, Benchmark> map = new HashMap<String, Benchmark>();
		for (String s : ss)
		{
			Benchmark bm = new Benchmark(s);
			map.put(Integer.toString(bm.getId()), bm);
		}
		return map;
	}
	
	/**
	 * Setter for id
	 */
	public void setId(int i)
	{
		bid = i;
	}
	
	/**
	 * Getter for id
	 */
	public int getId()
	{
		return bid;
	}
	
	/**
	 * Setter for code
	 */
	public void setCode(String c)
	{
		code = c;
	}
	
	/**
	 * Getter for code
	 */
	public String getCode()
	{
		return code;
	}
	
	/**
	 * Getter for longitude
	 */
	public double getLon()
	{
		return lon;
	}

	/**
	 * Getter for latitude
	 */
	public double getLat()
	{
		return lat;
	}
	
	/**
	 * Getter for height
	 */
	public double getHeight()
	{
		return height;
	}
	
	/**
	 * Get longitude/latitude coordinates as Point2D
	 */
	public Point2D.Double getLonLat()
	{
		return new Point2D.Double(lon, lat);
	}
	
	/**
	 * Get string benchmark representation, as code
	 */
	public String toString()
	{
		return code;
	}

	/**
	 * Get full string benchmark representation, as ':'-separated string 'id:code:longitude:latitude:height'
	 */
	public String toFullString()
	{
		return bid + ":" + + lon + ":" + lat + ":" + code + ":" + code + ":" + height;
	}
	
	/**
	 * Compare benchmarks by code
	 */
	public int compareTo(Object bm)
	{
		if (bm instanceof String)
			return code.compareTo((String)bm);
		else
			return code.compareTo(((Benchmark)bm).code);
	}
}
