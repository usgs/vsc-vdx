package gov.usgs.vdx.data;

import gov.usgs.util.ConfigFile;
import gov.usgs.vdx.db.VDXDatabase;
import gov.usgs.vdx.server.RequestResult;
import gov.usgs.vdx.server.TextResult;

import java.util.List;
import java.util.Map;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.3  2005/10/21 21:21:39  tparker
 * Roll back changes related to Bug #77
 *
 * Revision 1.1  2005/09/24 17:33:28  dcervelli
 * Initial commit.  Unused and experimental at this point.
 *
 * @author Dan Cervelli
 */
public class GenericSQLDataSource extends SQLDataSource
{
	protected String databaseName;
	protected String[] dataColumns;
	protected String type;
	
	public boolean createDatabase()
	{
		return createDefaultDatabase(name + "$" + databaseName, dataColumns.length, true, true);
	}

	public boolean databaseExists()
	{
		return defaultDatabaseExists(name + "$" + databaseName);
	}

	public String getType()
	{
		return type;
	}
	
	public void initialize(ConfigFile params)
	{
		String vdxHost = params.getString("vdx.host");
		String vdxName = params.getString("vdx.name");
		name = params.getString("vdx.databaseName");
		database = new VDXDatabase("com.mysql.jdbc.Driver", "jdbc:mysql://" + vdxHost + "/?user=vdx&password=vdx", vdxName);
	}

	public RequestResult getData(Map<String, String> params)
	{
		String action = params.get("action");
		if (action == null)
			return null;
		
		if (action.equals("selectors"))
		{
			List<String> s = getSelectors();
			return new TextResult(s);
		}
		else if (action.equals("data"))
		{}
		return null;
	}

	public List<String> getSelectors()
	{
		return defaultGetSelectors(databaseName);
	}

	public String getSelectorName(boolean plural)
	{
		return plural ? "Stations" : "Station";
	}
	
	public boolean createChannel(String channel, String channelName, double lon, double lat)
	{
		return createDefaultChannel(name + "$" + databaseName, dataColumns.length, channel, channelName, lon, lat, dataColumns, true, true);
	}
	
	public boolean insertData(int cid, double[][] data)
	{
		return false;
	}
}
