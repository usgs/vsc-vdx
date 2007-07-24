package gov.usgs.vdx.data.generic;

/**
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
	
	public GenericColumn()
	{}
	
	public GenericColumn(int i, String n, String d, String u, boolean b, boolean a)
	{
		index = i;
		name = n;
		description = d;
		unit = u;
		checked = b;
		active = a;
	}
	
	public GenericColumn(int i, String n, String d, String u, boolean b)
	{
		index = i;
		name = n;
		description = d;
		unit = u;
		checked = b;
		active = true;
	}
	
	public GenericColumn(String s)
	{
		String[] ss = s.split(":");
		index = Integer.parseInt(ss[0]);
		name = ss[1];
		description = ss[2];
		unit = ss[3];
		checked = ss[4].equals("T");
	}
	
	public String toString()
	{
		return(String.format("%d:%s:%s:%s:%s", index, name, description, unit, (checked ? "T" : "F")));
	}
}
