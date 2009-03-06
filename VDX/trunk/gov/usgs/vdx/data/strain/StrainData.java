package gov.usgs.vdx.data.strain;

import gov.usgs.vdx.data.GenericDataMatrix;

/**
 * Customized  GenericDataMatrix, with two columns: time and strain
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class StrainData extends GenericDataMatrix
{
	public void setColumnNames()
	{
		columnMap.put("time", 0);
		columnMap.put("strain", 1);
	}
}
