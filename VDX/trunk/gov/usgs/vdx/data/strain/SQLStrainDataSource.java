package gov.usgs.vdx.data.strain;

import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.db.VDXDatabase;
import gov.usgs.vdx.server.RequestResult;

import java.util.List;
import java.util.Map;

/**
 * 
 * $Log: not supported by cvs2svn $
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
	
	public void initialize(Map<String, Object> params)
	{
		database = (VDXDatabase)params.get("VDXDatabase");
		name = (String)params.get("name");
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
