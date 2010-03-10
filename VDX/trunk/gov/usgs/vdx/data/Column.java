package gov.usgs.vdx.data;

import java.util.ArrayList;
import java.util.List;

/**
 * Column is a class that stores attributes related to a data column in the database
 * and includes properties for how it is displayed in the UI.
 *
 * @author Dan Cervelli
 */
public class Column
{
	public int idx;
	public String name;
	public String description;
	public String unit;
	public boolean checked;
	public boolean active;
	
	/**
	 * Constructor
	 * @param i column index
	 * @param n name
	 * @param d description
	 * @param u measurement unit
	 * @param b is checked?
	 * @param a is active? (detrendable for plot columns)
	 */
	public Column(int i, String n, String d, String u, boolean b, boolean a) {
		idx			= i;
		name		= n;
		description	= d;
		unit		= u;
		checked		= b;
		active		= a;
	}

	/**
	 * Constructor.  Defaults active to true.
	 * @param i column index
	 * @param n name
	 * @param d description
	 * @param u measurement unit
	 * @param b is checked?
	 */
	public Column(int i, String n, String d, String u, boolean b) {
		idx 		= i;
		name		= n;
		description	= d;
		unit		= u;
		checked		= b;
		active		= true;
	}
	
	/**
	 * Constructor
	 * @param s ':'-separated string: index:name:description:unit:checked:active
	 */
	public Column(String s) {
		String[] ss	= s.split(":");
		idx			= Integer.parseInt(ss[0]);
		name		= ss[1];
		description	= ss[2];
		unit		= ss[3];
		checked		= ss[4].equals("T");
		active		= ss[5].equals("T");
	}
	
	/**
	 * Get string representation of class
	 */
	public String toString() {
		return(String.format("%d:%s:%s:%s:%s:%s", idx, name, description, unit, (checked ? "T" : "F"), (active ? "T" : "F")));
	}
	
	/**
	 * Conversion utility
	 * @param ss
	 * @return
	 */
	public static List<Column> fromStringsToList(List<String> ss) {
		List<Column> columns = new ArrayList<Column>();
		for (int i = 0; i < ss.size(); i++) {
			columns.add(new Column(ss.get(i)));
		}
		return columns;
	}
}
