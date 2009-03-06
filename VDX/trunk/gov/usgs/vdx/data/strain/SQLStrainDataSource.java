package gov.usgs.vdx.data.strain;

import gov.usgs.util.ConfigFile;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.db.VDXDatabase;
import gov.usgs.vdx.server.RequestResult;

import java.util.List;
import java.util.Map;

/**
 * Customized SQL data source to store strain data.
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

	/**
	 * Create database. 
	 * @return true if success
	 */
	public boolean createDatabase()
	{
		return createDefaultDatabase(name + "$" + DATABASE_NAME, DATA_COLUMNS.length, true, true);
	}

	/**
	 * Check if database exist.
	 */
	public boolean databaseExists()
	{
		return defaultDatabaseExists(name + "$" + DATABASE_NAME);
	}

	/**
	 * Getter for ds type
	 */
	public String getType()
	{
		return "strain";
	}
	
	/**
	 * Init this DataSource from configuration
	 */
	public void initialize(ConfigFile params)
	{
		String driver = params.getString("vdx.driver");
		String url = params.getString("vdx.url");
		String vdxPrefix = params.getString("vdx.vdxPrefix");
		if (vdxPrefix == null)
			throw new RuntimeException("config parameter vdx.vdxPrefix not found. Update config if using vdx.name");
		
		name = params.getString("vdx.name");
		if (name == null)
			throw new RuntimeException("config parameter vdx.name not found. Update config if using vdx.databaseName");

		database = new VDXDatabase(driver, url, vdxPrefix);
	}

	/**
	 * Not realized in this version
	 */
	public RequestResult getData(Map<String, String> params)
	{
		return null;
	}

	/**
	 * Get channels list in format "sid:code:name:lon:lat" from database
	 */
	public List<String> getSelectors()
	{
		return defaultGetSelectors(DATABASE_NAME);
	}

	/**
	 * Get selector name
	 * @param plural if we need selector name in the plural form
	 */
	public String getSelectorName(boolean plural)
	{
		return plural ? "Stations" : "Station";
	}
	
	/**
	 * Create channel
	 * @param channel channel code
	 * @param channelName channel name
	 * @param lon longitude
	 * @param lat latitude
	 * @return true if success
	 */
	public boolean createChannel(String channel, String channelName, double lon, double lat)
	{
		return createDefaultChannel(name + "$" + DATABASE_NAME, DATA_COLUMNS.length, channel, channelName, lon, lat, DATA_COLUMNS, true, true);
	}
}
