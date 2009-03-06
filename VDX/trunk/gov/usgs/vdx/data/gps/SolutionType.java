package gov.usgs.vdx.data.gps;

/**
 * Represent solution type
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class SolutionType
{
	private int stid;
	private String name;
	private int rank;
	
	/**
	 * Constructor
	 * @param id solution type id
	 * @param n name
	 * @param r rank
	 */
	public SolutionType(int id, String n, int r)
	{
		stid = id;
		name = n;
		rank = r;
	}
	
	/**
	 * Setter for solution type id
	 */
	public void setId(int i)
	{
		stid = i;
	}

	/**
	 * Getter for solution type id
	 */
	public int getId()
	{
		return stid;
	}
	
	/**
	 * Getter for solution type name
	 */
	public String getName()
	{
		return name;
	}
	
	/**
	 * Getter for solution type rank
	 */
	public int getRank()
	{
		return rank;
	}
	
	/**
	 * Get string representation - as name
	 */
	public String toString()
	{
		return name;
	}
}
