package gov.usgs.vdx.data;

import java.awt.geom.Point2D;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Dan Cervelli, Loren Antolik
 */
public class Channel {
	private int cid;
	private String code;
	private String name;
	private double lon;
	private double lat;
	private double height;
	private int ctid;
	
	/**
	 * Constructor
	 * @param cid		channel id
	 * @param code		channel code
	 * @param name		channel name
	 * @param lon		longitude
	 * @param lat		latitude
	 * @param height	height
	 * @param ctid		channel type id
	 */
	public Channel(int cid, String code, String name, double lon, double lat, double height, int ctid) {
		this.cid	= cid;
		this.code	= code;
		this.name	= name;
		this.lon	= lon;
		this.lat	= lat;
		this.height	= height;
		this.ctid	= ctid;
	}
	
	/**
	 * Constructor for not specifying the channel type id
	 * @param cid		channel id
	 * @param code		channel code
	 * @param name		channel name
	 * @param lon		longitude
	 * @param lat		latitude
	 * @param height	height
	 */
	public Channel(int cid, String code, String name, double lon, double lat, double height) {
		this(cid, code, name, lon, lat, height, 0);
	}
	
	public Channel(String ch) {
		String[] parts	= ch.split(":");
		
		cid		= Integer.parseInt(parts[0]);
		
		if (parts.length > 1) {
			code	= parts[1];
		} else {
			code	= null;
		}
		
		if (parts.length > 2) {
			name	= parts[2];
		} else {
			name	= code;
		}
		
		if (parts.length > 3) {
			lon		= Double.parseDouble(parts[3]);
		} else {
			lon		= Double.NaN;
		}
		
		if (parts.length > 4) {
			lat		= Double.parseDouble(parts[4]);
		} else {
			lat		= Double.NaN;
		}
		
		if (parts.length > 5) {
			height	= Double.parseDouble(parts[5]);
		} else {
			height	= Double.NaN;
		}
		
		if (parts.length > 6) {
			ctid	= Integer.parseInt(parts[6]);
		} else {
			ctid	= 0;
		}
	}

	/**
	 * Getter for channel id
	 */
	public int getId() {
		return cid;
	}

	/**
	 * Getter for channel code
	 */
	public String getCode() {
		return code;
	}

	/**
	 * Getter for channel name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Getter for channel Latitude
	 */
	public double getLat() {
		return lat;
	}

	/**
	 * Getter for channel Longitude
	 */
	public double getLon() {
		return lon;
	}
	
	/**
	 * Getter for channel Longitude and Latitude
	 * @return 2D Point
	 */
	public Point2D.Double getLonLat() {
		return new Point2D.Double(lon, lat);
	}

	/**
	 * Getter for channel height
	 */
	public double getHeight() {
		return height;
	}
	
	/**
	 * Getter for channel type id
	 */
	public int getCtid() {
		return ctid;
	}
	
	/**
	 * Conversion utility
	 * @param ss
	 * @return map of channels, keyed by channel id
	 */
	public static Map<Integer, Channel> fromStringsToMap(List<String> ss) {
		Map<Integer, Channel> map = new HashMap<Integer, Channel>();
		for (String s : ss) {
			Channel ch = new Channel(s);
			map.put(ch.getId(), ch);
		}
		return map;
	}
	
	/**
	 * Conversion of objects to string
	 */
	public String toString() {
		String lon, lat, height;
		if (Double.isNaN(getLon()))    { lon    = "NaN"; } else { lon    = String.valueOf(getLon()); }
		if (Double.isNaN(getLat()))    { lat    = "NaN"; } else { lat    = String.valueOf(getLat()); }
		if (Double.isNaN(getHeight())) { height = "NaN"; } else { height = String.valueOf(getHeight()); }
		return String.format("%d:%s:%s:%s:%s:%s:%d", getId(), getCode(), getName(), lon, lat, height, getCtid());
	}
}
