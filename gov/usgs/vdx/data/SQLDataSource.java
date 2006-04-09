package gov.usgs.vdx.data;

import gov.usgs.util.ConfigFile;
import gov.usgs.vdx.db.VDXDatabase;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * 
 * TODO: use PreparedStatements.
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.4  2005/09/21 18:13:58  dcervelli
 * Added defaultChannelExists().
 *
 * Revision 1.3  2005/09/06 21:36:43  dcervelli
 * Changed defaultGetSelectors() to standard VDX channel format.
 *
 * Revision 1.2  2005/09/01 00:28:56  dcervelli
 * Changes for default channels.
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
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
	
	public void setName(String n)
	{
		name = n;
	}
	
	abstract public void initialize(ConfigFile params);
	
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
			List<String> result = new ArrayList<String>();
			database.useDatabase(name + "$" + db);
			ResultSet rs = database.executeQuery("SELECT sid, code, name, lon, lat FROM channels ORDER BY code");
			if (rs != null)
			{
				while (rs.next())
				{
					result.add(String.format("%d:%f:%f:%s:%s", rs.getInt(1), rs.getDouble(4), rs.getDouble(5), rs.getString(2), rs.getString(3)));
				}
			}
			
			return result;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLDataSource.defaultGetSelectors() failed.", e);
		}
		return null;
	}
	
	public List<Channel> defaultGetChannels(String db)
	{
		try
		{
			List<Channel> result = new ArrayList<Channel>();
			database.useDatabase(db);
			ResultSet rs = database.executeQuery("SELECT sid, code, name, lon, lat FROM channels ORDER BY code");
			if (rs != null)
			{
				while (rs.next())
				{
					Channel ch = new Channel(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getDouble(4), rs.getDouble(5));
					result.add(ch);
				}
			}
			return result;
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLDataSource.defaultGetChannels() failed.", e);
		}
		return null;
	}
	
	public boolean defaultChannelExists(String db, String ch)
	{
		try
		{
			database.useDatabase(name + "$" + db);
			PreparedStatement ps = database.getPreparedStatement("SELECT COUNT(*) FROM channels WHERE code=?");
			ps.setString(1, ch);
			ResultSet rs = ps.executeQuery();
			rs.next();
			return rs.getInt(1) > 0;	
		}
		catch (SQLException e)
		{
			database.getLogger().log(Level.SEVERE, "SQLDataSource.defaultGetChannels() failed.", e);
		}
		return false;
	}
}
