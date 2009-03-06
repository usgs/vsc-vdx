package gov.usgs.vdx.data.generic.fixed;

/**
 * Describe screen table column
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2005/10/20 05:07:30  dcervelli
 * Initial commit.
 *
 * @author Dan Cervelli
 */
public class GenericColumn
{
	public int index;
	public String name;
	public String description;
	public String unit;
	public boolean checked;
	public boolean active;
	
	/**
	 * Default constructor
	 */
	public GenericColumn()
	{}
	
	/**
	 * Constructor
	 * @param i column index
	 * @param n name
	 * @param d description
	 * @param u measurement unit
	 * @param b is checked?
	 * @param a is active?
	 */
	public GenericColumn(int i, String n, String d, String u, boolean b, boolean a)
	{
		index = i;
		name = n;
		description = d;
		unit = u;
		checked = b;
		active = a;
	}

	/**
	 * Constructor
	 * @param i column index
	 * @param n name
	 * @param d description
	 * @param u measurement unit
	 * @param b is checked?
	 */
	public GenericColumn(int i, String n, String d, String u, boolean b)
	{
		index = i;
		name = n;
		description = d;
		unit = u;
		checked = b;
		active = true;
	}
	
	/**
	 * Constructor
	 * @param s ':'-separated string: index:name:description:unit:checked
	 */
	public GenericColumn(String s)
	{
		String[] ss = s.split(":");
		index = Integer.parseInt(ss[0]);
		name = ss[1];
		description = ss[2];
		unit = ss[3];
		checked = ss[4].equals("T");
	}
	
	/**
	 * Get string representation of class
	 */
	public String toString()
	{
		return(String.format("%d:%s:%s:%s:%s", index, name, description, unit, (checked ? "T" : "F")));
	}
}
