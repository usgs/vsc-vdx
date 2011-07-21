package gov.usgs.vdx.in;

/**
 * Representation of a column's value
 */
public class ColumnValue {
	
	public String columnName;
	public double columnValue;
	
	/**
	 * Constructor
	 * @param columnName name of column
	 * @param columnValue value of column
	 */
	public ColumnValue (String columnName, double columnValue) {
		this.columnName		= columnName;
		this.columnValue	= columnValue;
	}
}
