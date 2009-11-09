package gov.usgs.vdx.data.tilt;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
*We already have one station in package gov.usgs.vdx.data.generic.variable; why we need another one?
*The only visible difference is azimuth field.
*/

public class Station implements Comparable<Object> {
	
	private int sid;
	private String code;
	private String name;
	private double lon;
	private double lat;
	private double azimuth;
	
	/**
	 * Default constructor
	 */
	public Station () {
		sid		= -1;
		code	= null;
		name	= null;
		lon		= -999;
		lat		= -999;
		azimuth	= -999;
	}
	
	public Station (String station) {
		String[] parts = station.split(":");
		sid		= Integer.parseInt(parts[0]);
		code	= parts[3];
		name	= parts[4];
		lon		= Double.parseDouble(parts[1]);
		lat		= Double.parseDouble(parts[2]);
		azimuth	= Double.parseDouble(parts[5]);
	}
	
	public Station (int id, String c, String n, double ln, double lt, double az) {
		sid		= id;
		code	= c;
		name	= n;
		lon		= ln;
		lat		= lt;
		azimuth	= az;
	}
	
	public static List<Station> fromStringsToList(List<String> ss) {
		List<Station> stations = new ArrayList<Station>();
		for (String s : ss)
			stations.add(new Station(s));
		
		return stations;
	}

	public static Map<String, Station> fromStringsToMap(List<String> ss) {
		Map<String, Station> map = new HashMap<String, Station>();
		for (String s : ss) {
			Station station = new Station(s);
			map.put(Integer.toString(station.getId()), station);
		}
		return map;
	}
	
	public void setId(int i) {
		sid = i;
	}
	
	public int getId() {
		return sid;
	}
	
	public void setCode(String c) {
		code = c;
	}
	
	public String getCode() {
		return code;
	}
	
	public void setName(String c) {
		name = c;
	}
	
	public String getName() {
		return name;
	}
	
	public double getLon() {
		return lon;
	}
	
	public double getLat() {
		return lat;
	}
	
	public double getAzimuth()
	{
		return azimuth;
	}
	
	public Point2D.Double getLonLat() {
		return new Point2D.Double(lon, lat);
	}
	
	public String toString() {
		return code;
	}

	public String toFullString() {
		return sid + ":" + + lon + ":" + lat + ":" + code + ":" + name + ":" + azimuth;
	}
	
	public int compareTo(Object station) {
		if (station instanceof String)
			return code.compareTo((String)station);
		else
			return code.compareTo(((Station)station).code);
	}
}
