package gov.usgs.vdx.data.tilt;

import gov.usgs.vdx.data.DataSource;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.db.VDXDatabase;
import gov.usgs.vdx.server.RequestResult;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class SQLTiltDataSource extends SQLDataSource implements DataSource
{
	public static final String DATABASE_NAME = "tilt";
	
	public String getType()
	{
		return "tilt";
	}
	
	public void initialize(Map<String, Object> params)
	{
		database = (VDXDatabase)params.get("VDXDatabase");
		name = (String)params.get("name");
	}

	public boolean databaseExists()
	{
		return defaultDatabaseExists(name + "$" + DATABASE_NAME);
	}
	
	public boolean createDatabase()
	{
		try
		{
			Statement st = database.getStatement();
			database.useRootDatabase();
			String db = database.getDatabasePrefix() + "_" + name + "$" + DATABASE_NAME;
			st.execute("CREATE DATABASE " + db);
			st.execute("USE " + db);
			st.execute(
					"CREATE TABLE channels (sid INT PRIMARY KEY AUTO_INCREMENT," +
					"code VARCHAR(16) UNIQUE," +
					"name VARCHAR(255), " + 
					"lon DOUBLE, lat DOUBLE)");
			st.execute(
					"CREATE TABLE translations (tid INT PRIMARY KEY AUTO_INCREMENT," +
					"cx DOUBLE DEFAULT 1, cy DOUBLE DEFAULT 1," +
					"dx DOUBLE DEFAULT 0, dy DOUBLE DEFAULT 0," +
					"azimuth DOUBLE DEFAULT 0)");
			st.execute("INSERT INTO translations (tid) VALUES (1)");
			return true;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLTiltDataSource.createDatabase() failed.", e);
		}
		return false;
	}
	
	public boolean createChannel(String channel, String channelName, double lon, double lat)
	{
		try
		{
			Statement st = database.getStatement();
			database.useDatabase(name + "$" + DATABASE_NAME);
			String table = channel;
			st.execute(
					"INSERT INTO channels (code, name, lon, lat) VALUES ('" + channel + "','" + channelName + "'," + lon + "," + lat + ")");
			st.execute(
					"CREATE TABLE " + table + " (t DOUBLE PRIMARY KEY," +
					"x DOUBLE DEFAULT 0 NOT NULL, y DOUBLE DEFAULT 0 NOT NULL," +
					"tid INT DEFAULT 1 NOT NULL)");
			
			return true;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLTiltDataSource.createChannel(\"" + channel + "\", " + lon + ", " + lat + ") failed.", e);
		}
		return false;
	}

	public List<String> getSelectors()
	{
		return defaultGetSelectors(DATABASE_NAME);
	}

	public String getSelectorName(boolean plural)
	{
		return plural ? "Stations" : "Station";
	}

	public RequestResult getData(Map<String, String> params)
	{
		return null;
	}
}
