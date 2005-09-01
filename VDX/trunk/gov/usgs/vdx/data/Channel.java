package gov.usgs.vdx.data;

/**
 * 
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
	
	public Channel(int id, String c, String n, double lo, double la)
	{
		sid = id;
		code = c;
		name = n;
		lon = lo;
		lat = la;
	}

	public String getCode()
	{
		return code;
	}

	public double getLat()
	{
		return lat;
	}

	public double getLon()
	{
		return lon;
	}

	public String getName()
	{
		return name;
	}

	public int getId()
	{
		return sid;
	}
}
