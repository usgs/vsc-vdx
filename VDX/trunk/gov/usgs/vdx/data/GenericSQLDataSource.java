package gov.usgs.vdx.data;

import gov.usgs.vdx.db.VDXDatabase;
import gov.usgs.vdx.server.RequestResult;
import gov.usgs.vdx.server.TextResult;

import java.util.List;
import java.util.Map;

/**
 * 
 * $Log: not supported by cvs2svn $
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
	
	public void initialize(Map<String, Object> params)
	{
		database = (VDXDatabase)params.get("VDXDatabase");
		if (database == null)
		{
			String vdxHost = (String)params.get("vdx.host");
			String vdxName = (String)params.get("vdx.name");
			params.put("name", (String)params.get("vdx.databaseName"));
			database = new VDXDatabase("com.mysql.jdbc.Driver", "jdbc:mysql://" + vdxHost + "/?user=vdx&password=vdx", vdxName);
		}
		
		name = (String)params.get("name");
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
		{
			/*
			int bm = Integer.parseInt(params.get("bm"));
			double st = Double.parseDouble(params.get("st"));
			double et = Double.parseDouble(params.get("et"));
			int stid = Integer.parseInt(params.get("stid"));
			GPSData data = getGPSData(bm, stid, st, et);
			if (data != null)
				return new BinaryResult(data);
				
				*/
		}
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
