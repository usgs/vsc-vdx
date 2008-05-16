package gov.usgs.vdx.data;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.nwis.DataType;
import gov.usgs.vdx.data.nwis.Station;
import gov.usgs.vdx.db.VDXDatabase;
import gov.usgs.vdx.server.BinaryResult;
import gov.usgs.vdx.server.RequestResult;
import gov.usgs.vdx.server.TextResult;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import cern.colt.matrix.DoubleMatrix2D;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.3  2007/04/30 05:28:12  tparker
 * initial commit for rsam SQL Bob importer
 *
 * Revision 1.2  2007/04/25 08:03:16  tparker
 * cleanup
 *
 * Revision 1.1  2007/04/22 06:42:26  tparker
 * Initial ewrsam commit
 *
 * @author Tom Parker
 */
public class SQLNullDataSource extends SQLDataSource implements DataSource
{
	private static final String DATABASE_NAME = "null";
	
	public void initialize(ConfigFile params)
	{
	}
	
	public boolean createDatabase()
	{
		return true;
	}

	public boolean createChannel(String channel, String channelName, double lon, double lat)
	{
		return true;
	}
	
	public boolean databaseExists()
	{
		return true;
	}

	public String getType()
	{
		return "null";
	}

	public List<String> getSelectors()
	{
		return null;
	}

	public String getSelectorName(boolean plural)
	{
		return null;
	}
	
	public RequestResult getData(Map<String, String> params)
	{		
		return null;
	}
	
	public void insertData(String channel, DoubleMatrix2D data, boolean r)
	{
			for (int i=0; i < data.rows(); i++)
				System.out.println(Util.j2KToDateString(data.getQuick(i, 0)) + " : " + data.getQuick(i, 1));
	}
}