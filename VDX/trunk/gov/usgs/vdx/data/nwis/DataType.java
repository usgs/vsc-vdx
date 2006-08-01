package gov.usgs.vdx.data.nwis;

/**
 * 
 * $Log: not supported by cvs2svn $
 *
 * @author Tom Parker
 */
public class DataType
{
	private int stid;
	private String name;
	
	public DataType(int id, String n)
	{
		stid = id;
		name = n;
	}
	
	public void setId(int i)
	{
		stid = i;
	}
	
	public int getId()
	{
		return stid;
	}
	
	public String getName()
	{
		return name;
	}
	
	public String toString()
	{
		return name;
	}
}
