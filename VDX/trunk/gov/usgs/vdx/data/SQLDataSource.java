package gov.usgs.vdx.data;

import gov.usgs.vdx.db.VDXDatabase;

import java.sql.ResultSet;
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
abstract public class SQLDataSource
{
	protected VDXDatabase database;
	protected String name = "default";
	
	public void setDatabase(VDXDatabase db)
	{
		database = db;
	}
	
	abstract public void initialize(Map<String, Object> params);
	
	abstract public boolean createDatabase();
	
	abstract public boolean databaseExists();
	
	public boolean defaultDatabaseExists(String dbName)
	{
		return database.useDatabase(dbName);
	}
	
	public boolean createDefaultDatabase(String dbName, int comps, boolean channels, boolean translations)
	{
		try
		{
			database.useRootDatabase();
			Statement st = database.getStatement();
			String db = database.getDatabasePrefix() + "_" + dbName;
			st.execute("CREATE DATABASE " + db);
			st.execute("USE " + db);
			if (channels)
			{
				st.execute(
						"CREATE TABLE channels (sid INT PRIMARY KEY AUTO_INCREMENT," +
						"code VARCHAR(16) UNIQUE," +
						"name VARCHAR(255), " + 
						"lon DOUBLE, lat DOUBLE)");
				if (translations)
				{
					String sql = "";
					for (int i = 0; i < comps; i++)
					{
						sql += "c" + i + " DOUBLE DEFAULT 1, d" + i + " DOUBLE DEFAULT 0";
						if (i != comps - 1)
							sql += ",";
					}
					st.execute(
							"CREATE TABLE translations (tid INT PRIMARY KEY AUTO_INCREMENT," +
							sql + ")");
					st.execute("INSERT INTO translations (tid) VALUES (1)");
				}
			}
			return true;
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			database.getLogger().log(Level.SEVERE, "SQLDataSource.createDatabase() failed.", e);
		}
		return false;
	}
	
	/**
	 * A default implementation is provided here because there are potentially
	 * data sources that would not support this method (example: hypocenter
	 * catalog).
	 * 
	 * @param channel
	 * @param lon
	 * @param lat
	 * @return
	 */
	public boolean createChannel(String channel, String channelName, double lon, double lat)
	{
		return false;
	}
	
	public boolean createDefaultChannel(String dbName, int comps, String channel, String channelName, double lon, double lat, String[] cols, boolean channels, boolean translations)
	{
		try
		{
			if (channelName == null)
				channelName = "";
			Statement st = database.getStatement();
			database.useDatabase(dbName);
			if (channels)
			{
				st.execute(
						"INSERT INTO channels (code, name, lon, lat) VALUES ('" + channel + "','" + channelName + "'," + lon + "," + lat + ")");
			}
			if (cols == null)
			{
				cols = new String[comps];
				for (int i = 0; i < comps; i++)
					cols[i] = "ch" + i;
			}
			
			String table = channel;
			String sql = "CREATE TABLE " + table + " (t DOUBLE PRIMARY KEY,";
			for (int i = 0; i < comps; i++)
			{
				sql += cols[i] + " DOUBLE DEFAULT 0 NOT NULL";
				if (i != comps - 1)
					sql += ",";
			}
			if (translations)
				sql += ",tid INT DEFAULT 1 NOT NULL)";
			else
				sql += ")";
			st.execute(sql);
			return true;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLDataSource.createDefaultChannel(\"" + channel + "\", " + lon + ", " + lat + ") failed.", e);
		}
		return false;
	}
	
	public List<String> defaultGetSelectors(String db)
	{
		try
		{
			List<String> result = null;
			database.useDatabase(db);
			ResultSet rs = database.executeQuery("SELECT code FROM channels ORDER BY code");
			if (rs != null)
			{
				while (rs.next())
					result.add(rs.getString(1));
			}
			
			return result;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLDataSource.defaultGetSelectors() failed.", e);
		}
		return null;
	}
}
