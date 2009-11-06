package gov.usgs.vdx.data;

// import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.vdx.db.VDXDatabase;
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
	public static final boolean channels		= false;
	public static final boolean translations	= false;
	public static final boolean channelTypes	= false;
	public static final boolean ranks			= false;
	public static final boolean columns			= false;

	public String getType() { return DATABASE_NAME; }	
	public boolean getChannelsFlag() { return channels; }
	public boolean getTranslationsFlag() { return translations; }
	public boolean getChannelTypesFlag() { return channelTypes; }
	public boolean getRanksFlag() { return ranks; }
	public boolean getColumnsFlag() { return columns; }

	public void initialize(VDXDatabase db, String dbName) {
		defaultInitialize(db, dbName + "$" + getType());
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
	
	public void insertData(String channel, DoubleMatrix2D data, boolean r)
	{
			for (int i=0; i < data.rows(); i++)
				System.out.println(Util.j2KToDateString(data.getQuick(i, 0)) + " : " + data.getQuick(i, 1));
	}
}