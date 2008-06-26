package gov.usgs.vdx.data.strain;

import gov.usgs.util.ConfigFile;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.db.VDXDatabase;
import gov.usgs.vdx.server.RequestResult;

import java.util.List;
import java.util.Map;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class SQLStrainDataSource extends SQLDataSource
{
	public static final String DATABASE_NAME = "strain";
	public static final String[] DATA_COLUMNS = new String[] {"strain"};
	
	public boolean createDatabase()
	{
		return createDefaultDatabase(name + "$" + DATABASE_NAME, DATA_COLUMNS.length, true, true);
	}

	public boolean databaseExists()
	{
		return defaultDatabaseExists(name + "$" + DATABASE_NAME);
	}

	public String getType()
	{
		return "strain";
	}
	
	public void initialize(ConfigFile params)
	{
		String vdxHost = params.getString("vdx.host");
		String vdxPrefix = params.getString("vdx.vdxPrefix");
		if (vdxPrefix == null)
			throw new RuntimeException("config parameter vdx.vdxPrefix not found. Update config if using vdx.name");
		
		name = params.getString("vdx.name");
		if (name == null)
			throw new RuntimeException("config parameter vdx.name not found. Update config if using vdx.databaseName");

		database = new VDXDatabase("com.mysql.jdbc.Driver", "jdbc:mysql://" + vdxHost + "/?user=vdx&password=vdx", vdxPrefix);
	}

	public RequestResult getData(Map<String, String> params)
	{
		return null;
	}

	public List<String> getSelectors()
	{
		return defaultGetSelectors(DATABASE_NAME);
	}

	public String getSelectorName(boolean plural)
	{
		return plural ? "Stations" : "Station";
	}
	
	public boolean createChannel(String channel, String channelName, double lon, double lat)
	{
		return createDefaultChannel(name + "$" + DATABASE_NAME, DATA_COLUMNS.length, channel, channelName, lon, lat, DATA_COLUMNS, true, true);
	}
}
