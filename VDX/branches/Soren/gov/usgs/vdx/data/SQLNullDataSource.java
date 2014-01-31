package gov.usgs.vdx.data;

import gov.usgs.util.ConfigFile;
import gov.usgs.vdx.server.RequestResult;

import java.util.List;
import java.util.Map;

/**
 * Stub class, it dumps information on stdout instead of insert it into the database
 *
 * @author Tom Parker
 */
public class SQLNullDataSource extends SQLDataSource implements DataSource
{
	private static final String DATABASE_NAME 	= "null";
	public static final boolean channels		= false;
	public static final boolean translations	= false;
	public static final boolean channelTypes	= false;
	public static final boolean ranks			= false;
	public static final boolean columns			= false;
	public static final boolean menuColumns		= false;

	/**
	 * Get database type, generic in this case
	 * return type
	 */
	public String getType() 				{ return DATABASE_NAME; }	
	public boolean getChannelsFlag()		{ return channels; }
	public boolean getTranslationsFlag()	{ return translations; }
	public boolean getChannelTypesFlag()	{ return channelTypes; }
	public boolean getRanksFlag()			{ return ranks; }
	public boolean getColumnsFlag()			{ return columns; }
	public boolean getMenuColumnsFlag()		{ return menuColumns; }
	
	/**
	 * Initialize data source
	 * @param params config file
	 */
	public void initialize(ConfigFile params) {
		defaultInitialize(params);
		if (!databaseExists()) {
			createDatabase();
		}
	}
	
	/**
	 * De-Initialize data source
	 */
	public void disconnect() {
		defaultDisconnect();
	}
	
	/**
	 * Create database
	 * @return true
	 */
	public boolean createDatabase()
	{
		return true;
	}

	/**
	 * Create channel
	 * @param channel ???
	 * @param channelName channel name
	 * @param lon longitude
	 * @param lat latitude
	 * @return true
	 */
	public boolean createChannel(String channel, String channelName, double lon, double lat)
	{
		return true;
	}
	
	/**
	 * Does database exist?
	 * @return true
	 */
	public boolean databaseExists()
	{
		return true;
	}

	/**
	 * Get list of channels
	 * @return null
	 */
	public List<String> getChannels()
	{
		return null;
	}

	/**
	 * Get channel name
	 * @param plural ???
	 * @return null
	 */
	public String getChannelName(boolean plural)
	{
		return null;
	}
	
	/**
	 * Get data
	 * @param params mapping of params to their values
	 * @return null
	 */
	public RequestResult getData(Map<String, String> params)
	{		
		return null;
	}
}