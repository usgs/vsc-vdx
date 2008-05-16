package gov.usgs.vdx.data.nwis;

import gov.usgs.vdx.data.GenericDataMatrix;

import java.util.List;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2006/08/01 19:54:47  tparker
 * Create NWIS data source
 *
 *
 * @author Tom Parker
 */
public class NWISData extends GenericDataMatrix
{
	public NWISData()
	{
		super();
	}
	
	public NWISData(List<double[]> pts)
	{
		super(pts);
	}
	
	public void setColumnNames()
	{
		columnMap.put("time", 0);
		columnMap.put("type", 1);
		columnMap.put("value", 2);
	}
}
