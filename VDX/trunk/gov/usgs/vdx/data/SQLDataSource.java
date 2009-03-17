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
import java.util.logging.Logger;

import cern.colt.matrix.DoubleMatrix2D;

/**
 * Generic SQL data source.
 * Store reference to VDX database and provide methods to init
 * default database structure.
 * 
 * TODO: use PreparedStatements.
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.5  2006/04/09 18:26:05  dcervelli
 * ConfigFile/type safety changes.
 *
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
	protected Logger logger;
	protected String name = "default";
	protected String driver;
	protected String url;
	protected String vdxPrefix;
	
	/**
	 * Sets VDX database for this source
	 */
	public void setDatabase(VDXDatabase db)
	{
		database = db;
		logger = database.getLogger();
	}

	/**
	 * Sets name of this SQLDataSource
	 */
	public void setName(String n)
	{
		name = n;
	}
	
	/**
	 * Gets name of this SQLDataSource
	 * @name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Init this SQLDataSource from configuration
	 */
	abstract public void initialize(ConfigFile params);
	
	/**
	 * Create database. Concrete realization see in the inherited classes
	 * @return true if success
	 */
	abstract public boolean createDatabase();
	
	/**
	 * Check if database exist. Concrete realization see in the inherited classes
	 */
	abstract public boolean databaseExists();
	
	/**
	 * Check if VDX database has connection to SQL server
	 * @param dbName
	 * @return
	 */
	public boolean defaultDatabaseExists(String dbName)
	{
		return database.useDatabase(dbName);
	}
	
	/**
	 * Create default VDX database
	 * @param dbName database name (without prefix)
	 * @param comps number of places in translation table
	 * @param channels if we need to create channels table
	 * @param translations if we need to create translations table
	 * @return
	 */
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
			logger.log(Level.SEVERE, "SQLDataSource.createDatabase() failed.", e);
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
	 * @return false, creation was not succeded
	 */
	public boolean createChannel(String channel, String channelName, double lon, double lat)
	{
		return false;
	}

	/**
	 * Create default channel
	 * @param dbName database name (without prefix)
	 * @param channel channel code
	 * @param channelName channel name
	 * @param lon longitude
	 * @param lat latitude
	 * @param cols array of strings - column names in table created to store channel data
	 * @param channels if we need to add record in 'channels' table
	 * @param translations if we need to add 'tid' field to table created to store channel data
	 * @return true if success
	 */
	public boolean createDefaultChannel(String dbName, int comps, String channel, String channelName, double lon, double lat, 
			String[] cols, boolean channels, boolean translations) {
		
		logger.info("Creating channel " + channel);
		try {
			if (channelName == null)
				channelName = "";
			Statement st = database.getStatement();
			database.useDatabase(dbName);
			if (channels) {
				st.execute("INSERT INTO channels (code, name, lon, lat) " +
						   "VALUES ('" + channel + "','" + channelName + "'," + lon + "," + lat + ")");
			}
			if (cols == null) {
				cols = new String[comps];
				for (int i = 0; i < comps; i++) {
					cols[i] = "ch" + i;
				}
			}
			
			String table = channel;
			String sql = "CREATE TABLE " + table + " (t DOUBLE PRIMARY KEY,";
			for (int i = 0; i < comps; i++) {
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
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "SQLDataSource.createDefaultChannel(\"" + channel + "\", " + lon + ", " + lat + ") failed.", e);
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * Get channels list in format "sid:code:name:lon:lat" from database
	 * @param db database name to query
	 */
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
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetSelectors() failed.", e);
		}
		return null;
	}

	/**
	 * Get channels list from database
	 * @param db database name to query
	 */
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
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannels() failed.", e);
		}
		return null;
	}
	
	/**
	 * Check if channel exist
	 * @param db database to check (without prefix)
	 * @param ch channel code to check
	 */
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
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannels() failed.", e);
		}
		return false;
	}

	/**
	 * Import channel data, not implemented in this generic class
	 * @param channel channel code
	 * @param data data matrix
	 */
	public void insertData(String channel, DoubleMatrix2D data)
	{
		insertData(channel, data, false);
	}

	/**
	 * Import channel data, not implemented in this generic class
	 * @param channel channel code
	 * @param data data matrix
	 * @param b
	 */
	public void insertData(String channel, DoubleMatrix2D data, boolean b)
	{
		System.out.println("Data import not available for this source.");
	}

}