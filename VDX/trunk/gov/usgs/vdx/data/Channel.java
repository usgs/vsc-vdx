package gov.usgs.vdx.data;

/**
 * Represent channel
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class Channel
{
	private int sid;
	private String code;
	private String name;
	private double lon;
	private double lat;
	
	/**
	 * Constructor
	 * @param id sid
	 * @param c channel code
	 * @param n channel name
	 * @param lo longitude
	 * @param la latitude
	 */
	public Channel(int id, String c, String n, double lo, double la)
	{
		sid = id;
		code = c;
		name = n;
		lon = lo;
		lat = la;
	}

	/**
	 * Getter for channel code
	 */
	public String getCode()
	{
		return code;
	}

	/**
	 * Getter for channel Latitude
	 */
	public double getLat()
	{
		return lat;
	}

	/**
	 * Getter for channel Longitude
	 */
	public double getLon()
	{
		return lon;
	}

	/**
	 * Getter for channel name
	 */
	public String getName()
	{
		return name;
	}

	/**
	 * Getter for channel id
	 */
	public int getId()
	{
		return sid;
	}
}
