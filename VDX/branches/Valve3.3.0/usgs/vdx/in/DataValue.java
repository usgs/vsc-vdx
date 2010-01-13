package gov.usgs.vdx.in;

public class DataValue {
	
	public String columnName;
	public double columnValue;
	
	public DataValue (String columnName, double columnValue) {
		this.columnName		= columnName;
		this.columnValue	= columnValue;
	}
}
