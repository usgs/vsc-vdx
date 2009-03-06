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
 * Revision 1.4  2006/04/09 18:22:29  dcervelli
 * ConfigFile/type safety changes.
 *
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

	/**
	 * Get flag if database exists
	 */
	public boolean databaseExists()
	{
		return defaultDatabaseExists(name + "$" + databaseName);
	}

	/**
	 * Getter for type
	 */
	public String getType()
	{
		return type;
	}
	
	/**
	 * Initialize this data source from given configuration
	 */
	public void initialize(ConfigFile params)
	{
		String vdxPrefix = params.getString("vdx.vdxPrefix");
		if (vdxPrefix == null)
			throw new RuntimeException("config parameter vdx.vdxPrefix not found. Update config if using vdx.name.");

		name = params.getString("vdx.name");
		if (name == null)
			throw new RuntimeException("config parameter vdx.name not found. Update config if using vdx.dabaseName.");

		database = new VDXDatabase(driver, url, vdxPrefix);
	}

	/**
	 * Retrieve data from data source
	 * @param params command as map of strings for 'parameter-value' pairs to specify query
	 * @return retrieved result of command execution
	 */
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

	/**
	 * Get channels list in format "sid:code:name:lon:lat" from database
	 */
	public List<String> getSelectors()
	{
		return defaultGetSelectors(databaseName);
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
	 */
	public boolean createChannel(String channel, String channelName, double lon, double lat)
	{
		return createDefaultChannel(name + "$" + databaseName, dataColumns.length, channel, channelName, lon, lat, dataColumns, true, true);
	}
	
	/**
	 * Not realized in this version
	 * @param cid
	 * @param data
	 * @return
	 */
	public boolean insertData(int cid, double[][] data)
	{
		return false;
	}
}
