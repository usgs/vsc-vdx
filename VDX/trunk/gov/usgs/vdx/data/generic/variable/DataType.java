package gov.usgs.vdx.data.generic.variable;

/**
 * Represents data type
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
	
	/**
	 * Constructor
	 * @param id data type id
	 * @param n name
	 */
	public DataType(int id, String n)
	{
		stid = id;
		name = n;
	}
	
	/**
	 * Setter for data type id
	 */
	public void setId(int i)
	{
		stid = i;
	}
	
	/**
	 * Getter for data type id
	 */
	public int getId()
	{
		return stid;
	}
	
	public String getName()
	{
		return name;
	}
	
	/**
	 * Getter for data type name
	 */
	public String toString()
	{
		return name;
	}
	
	/**
	 * Compare data types by id
	 */
	public boolean equals(int i)
	{
		return i == stid ? true : false;
	}
}
