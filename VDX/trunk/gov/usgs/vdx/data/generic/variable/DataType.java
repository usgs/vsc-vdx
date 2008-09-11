package gov.usgs.vdx.data.generic.variable;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2006/08/01 19:54:47  tparker
 * Create NWIS data source
 *
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
	
	public boolean equals(int i)
	{
		return i == stid ? true : false;
	}
}
