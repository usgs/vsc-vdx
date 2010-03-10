package gov.usgs.vdx.data.generic.variable;

import gov.usgs.vdx.data.GenericDataMatrix;

import java.util.List;

/**
 * GenericDataMatrix with columns time, type, value
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2006/08/01 19:54:47  tparker
 * Create NWIS data source
 *
 *
 * @author Tom Parker
 */
public class GenericVariableData extends GenericDataMatrix
{
	/**
	 * Default constructor
	 */
	public GenericVariableData()
	{
		super();
	}
	
	/**
	 * Constructor
	 * @param pts 2d matrix of data to init
	 */
	public GenericVariableData(List<double[]> pts)
	{
		super(pts);
	}
	
	/**
	 * Set column names: time/type/value
	 */
	public void setColumnNames()
	{
		columnMap.put("time", 0);
		columnMap.put("type", 1);
		columnMap.put("value", 2);
	}
}
