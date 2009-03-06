package gov.usgs.vdx.data;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.vdx.server.RequestResult;

import java.util.List;
import java.util.Map;

import cern.colt.matrix.DoubleMatrix2D;

/**
 * Stub class, it dumps information on stdout instead of insert it into the database
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