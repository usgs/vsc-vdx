package gov.usgs.vdx.data.nwis;

import gov.usgs.vdx.data.GenericDataMatrix;
import gov.usgs.vdx.server.BinaryResult;
import gov.usgs.vdx.server.RequestResult;
import gov.usgs.vdx.server.TextResult;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import cern.colt.function.DoubleDoubleFunction;
import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix1D;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.linalg.Algebra;

/**
 * 
 * $Log: not supported by cvs2svn $
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
