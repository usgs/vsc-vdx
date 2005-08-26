package gov.usgs.vdx.data.gps;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class SolutionType
{
	private int stid;
	private String name;
	private int rank;
	
	public SolutionType(int id, String n, int r)
	{
		stid = id;
		name = n;
		rank = r;
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
	
	public int getRank()
	{
		return rank;
	}
	
	public String toString()
	{
		return name;
	}
}
