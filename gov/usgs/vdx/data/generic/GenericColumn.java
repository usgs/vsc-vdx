package gov.usgs.vdx.data.generic;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class GenericColumn
{
	public int index;
	public String name;
	public String description;
	public String unit;
	public boolean checked;
	
	public GenericColumn()
	{}
	
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
