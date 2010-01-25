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
	 */
	public void initialize(ConfigFile params) {
		defaultInitialize(params);
		if (!databaseExists()) {
			createDatabase();
		}
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

	public List<String> getChannels()
	{
		return null;
	}

	public String getChannelName(boolean plural)
	{
		return null;
	}
	
	public RequestResult getData(Map<String, String> params)
	{		
		return null;
	}
	
	public void insertData(String channelCode, GenericDataMatrix gdm, boolean translations, boolean ranks, int rid)
	{
		DoubleMatrix2D data			= gdm.getData();
		for (int i=0; i < data.rows(); i++)
			System.out.println(Util.j2KToDateString(data.getQuick(i, 0)) + " : " + data.getQuick(i, 1));
	}
}