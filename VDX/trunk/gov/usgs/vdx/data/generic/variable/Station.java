package gov.usgs.vdx.data.generic.variable;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.2  2006/09/20 23:14:38  tparker
 * Add active column to DB
 *
 * Revision 1.1  2006/08/01 19:54:47  tparker
 * Create NWIS data source
 *
 *
 * @author Tom Parker
 */
public class Station implements Comparable<Object>
{
	private int sid;
	private String org;
	private String siteNo;
	private String name;
	private double lon;
	private double lat;
	private String tz;
	private boolean active;
	
	public Station()
	{
		sid = -1;
		org = null;
		siteNo = null;
		name = null;
		lon = -999;
		lat = -999;
		tz = "GMT";
		active = true;
	}
	
	public Station(String s)
	{
		String[] parts = s.split(":");
		sid = Integer.parseInt(parts[0]);
		org = parts[4];
		siteNo = parts[5];
		name = parts[6];
		lon = Double.parseDouble(parts[1]);
		lat = Double.parseDouble(parts[2]);
		active = true;
	}
	
	public Station(int id, String o, String s, String n, double ln, double lt, String t, boolean a)
	{
		sid = id;
		org = o;
		siteNo = s;
		name = n;
		lon = ln;
		lat = lt;
		tz = t;
		active = a;
	}
	
	public static List<Station> fromStringsToList(List<String> ss)
	{
		List<Station> stations = new ArrayList<Station>();
		for (String s : ss)
			stations.add(new Station(s));
		
		return stations;
	}

	public static Map<String, Station> fromStringsToMap(List<String> ss)
	{
		Map<String, Station> map = new HashMap<String, Station>();
		for (String s : ss)
		{
			Station station = new Station(s);
			map.put(Integer.toString(station.getId()), station);
		}
		return map;
	}
	
	public void setId(int i)
	{
		sid = i;
	}
	
	public int getId()
	{
		return sid;
	}
	
	public void setOrg(String o)
	{
		org = o;
	}
	
	public String getOrg()
	{
		return org;
	}
	
	public void setSiteNo(String s)
	{
		siteNo = s;
	}
	
	public String getSiteNo()
	{
		return siteNo;
	}
	
	public void setName(String n)
	{
		name = n;
	}
	
	public String getName()
	{
		return name;
	}
	
	public double getLon()
	{
		return lon;
	}
	
	public double getLat()
	{
		return lat;
	}

	public String getTz()
	{
		return tz;
	}
	
	public boolean getActive()
	{
		return active;
	}
	
	public Point2D.Double getLonLat()
	{
		return new Point2D.Double(lon, lat);
	}
	
	public String toString()
	{
		return org + siteNo;
	}

	public String toFullString()
	{
		return sid + ":" + lon + ":" + lat + ":" + tz + ":" + org + ":" + siteNo + ":" + name;
	}
	
	public int compareTo(Object s)
	{
		if (s instanceof String)
			return siteNo.compareTo((String)s);
		else
			return siteNo.compareTo(((Station)s).siteNo);
	}
}
