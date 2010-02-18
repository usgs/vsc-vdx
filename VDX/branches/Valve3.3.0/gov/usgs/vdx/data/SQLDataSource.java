package gov.usgs.vdx.data;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.vdx.db.VDXDatabase;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;

import cern.colt.matrix.DoubleMatrix2D;

/**
 * SQL data source. 
 * Store reference to VDX database and provide methods to init default database structure.
 * 
 * TODO: use Statements for low rate queries.
 * 
 * @author Dan Cervelli, Loren Antolik
 */
abstract public class SQLDataSource {
	
	protected VDXDatabase database;
	protected String dbName;
	protected Logger logger;

	protected Statement st;
	protected PreparedStatement ps;
	protected ResultSet rs;
	protected String sql;
	
	/**
	 * Initialize the data source.  Concrete realization see in the inherited classes
	 */
	abstract public void initialize(ConfigFile params);
	
	/**
	 * Get database suffix. Concrete realization see in the inherited classes
	 * @return type string
	 */
	abstract public String getType();
	
	abstract public boolean getChannelsFlag();
	abstract public boolean getTranslationsFlag();
	abstract public boolean getRanksFlag();
	abstract public boolean getColumnsFlag();
	abstract public boolean getMenuColumnsFlag();
	abstract public boolean getChannelTypesFlag();

	/**
	 * Check if database exist. Concrete realization see in the inherited classes
	 * @return true if success
	 */
	abstract public boolean databaseExists();

	/**
	 * Create database. Concrete realization see in the inherited classes
	 * @return true if success
	 */
	abstract public boolean createDatabase();
	
	/**
	 * Insert data.  Concrete realization see in the inherited classes
	 * @return true if success
	 */
	// abstract public void insertData(String channelCode, GenericDataMatrix gdm, boolean translations, boolean ranks, int rid);
	
	/**
	 * Initialize Data Source
	 * 
	 * @param db		VDXDatabase object
	 * @param dbName	name of the database, minus the prefix
	 */
	public void defaultInitialize(ConfigFile params) {
		
		// common database connection parameters
		String driver	= params.getString("vdx.driver");
		String url		= params.getString("vdx.url");
		String prefix	= params.getString("vdx.prefix");
		database		= new VDXDatabase(driver, url, prefix);
		
		// dbName is an additional parameter that VDX classes uses, unlike Winston or Earthworm
		dbName			= params.getString("vdx.name") + "$" + getType();
		
		// initialize the logger for this data source
		logger			= Logger.getLogger("gov.usgs.vdx.data.SQLDataSource");
		logger.log(Level.INFO, "SQLDataSource.defaultInitialize(" + database.getDatabasePrefix() + "_" + dbName + ") succeeded.");
	}

	/**
	 * Close database connection
	 */
	public void disconnect() {
		database.close();
	}

	/**
	 * Check if VDX database has connection to SQL server
	 * 
	 * @return true if successful
	 */
	public boolean defaultDatabaseExists() {
		return database.useDatabase(dbName);
	}

	/**
	 * Create default VDX database
	 * 
	 * @param channels		if we need to create channels table
	 * @param translations	if we need to create translations table
	 * @param channelTypes	if we need to create channel_types table
	 * @param ranks			if we need to create ranks table
	 * @param columns		if we need to create columns table
	 * @return true if success
	 */
	public boolean defaultCreateDatabase(boolean channels, boolean translations, boolean channelTypes, boolean ranks, boolean columns, boolean menuColumns) {
		try {
			
			// create the database on the database server and specify to use this database for all subsequent statements
			database.useRootDatabase();
			ps = database.getPreparedStatement("CREATE DATABASE " + database.getDatabasePrefix() + "_" + dbName);
			ps.execute();
			database.useDatabase(dbName);

			// creation of a channels table
			if (channels) {

				// these are the basic channel options, we can add on to this below
				sql = "CREATE TABLE channels (cid INT PRIMARY KEY AUTO_INCREMENT, "
						+ "code VARCHAR(16) UNIQUE, name VARCHAR(255), "
						+ "lon DOUBLE, lat DOUBLE, height DOUBLE";

				// translations. logically you must have a channels table to have translations
				if (translations) {
					sql = sql + ", tid INT DEFAULT 1 NOT NULL";
				}

				// channel types. logically you must have a channels table to have channel types
				if (channelTypes) {
					sql = sql + ", ctid INT DEFAULT 1 NOT NULL";
					ps.execute("CREATE TABLE channel_types (ctid INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(16) UNIQUE)");
					ps.execute("INSERT INTO channel_types (name) VALUES ('DEFAULT')");
				}

				// complete the channels sql statement and execute it
				sql = sql + ")";
				ps.execute(sql);
			}

			if (columns) {
				ps.execute("CREATE TABLE columns (colid INT PRIMARY KEY AUTO_INCREMENT, "
					+ "idx INT, name VARCHAR(255) UNIQUE, description VARCHAR(255), "
					+ "unit VARCHAR(255), checked TINYINT, active TINYINT)");
			}

			if (menuColumns) {
				ps.execute("CREATE TABLE columns_menu (colid INT PRIMARY KEY AUTO_INCREMENT, "
					+ "idx INT, name VARCHAR(255) UNIQUE, description VARCHAR(255), "
					+ "unit VARCHAR(255), checked TINYINT, active TINYINT)");
			}

			// the usage of ranks does not depend on there being a channels table
			if (ranks) {
				ps.execute("CREATE TABLE ranks (rid INT PRIMARY KEY AUTO_INCREMENT,"
					+ "name VARCHAR(24) UNIQUE, rank INT(10) UNSIGNED DEFAULT 0 NOT NULL, user_default TINYINT(1) DEFAULT 0 NOT NULL)");
			}
			
			logger.log(Level.INFO, "SQLDataSource.defaultCreateDatabase(" + database.getDatabasePrefix() + "_" + dbName + ") succeeded. ");
			return true;

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultCreateDatabase(" + database.getDatabasePrefix() + "_" + dbName + ") failed.", e);
		}

		return false;
	}

	/**
	 * Get channel name
	 * 
	 * @param plural	if we need channel name in the plural form
	 */
	public String getChannelName(boolean plural) {
		return plural ? "Channels" : "Channel";
	}
	
	/**
	 * Create default channel from values in the columns table
	 * 
	 * @param channel		channel object
	 * @param tid			translation id
	 * @param channels		if we need to add record in 'channels' table
	 * @param translations	if we need to add tid field in channel table
	 * @param ranks			if we need to add rid field in channel table
	 * @param columns		if we need to create a channel table based on the columns table
	 * @return true if success
	 */
	public boolean defaultCreateChannel(Channel channel, int tid, boolean channels, boolean translations, boolean ranks, boolean columns) {
		return defaultCreateChannel(channel.getCode(), channel.getName(), channel.getLon(), channel.getLat(), channel.getHeight(), tid,
				channels, translations, ranks, columns);
	}

	/**
	 * Create default channel from values in the columns table
	 * 
	 * @param channelCode	channel code
	 * @param channelName	channel name
	 * @param lon			longitude
	 * @param lat			latitude
	 * @param height		height
	 * @param tid			translation id
	 * @param channels		if we need to add record in 'channels' table
	 * @param translations	if we need to add tid field in channel table
	 * @param ranks			if we need to add rid field in channel table
	 * @param columns		if we need to create a channel table based on the columns table
	 * @return true if success
	 */
	public boolean defaultCreateChannel(String channelCode, String channelName, 
			double lon, double lat, double height, int tid,
			boolean channels, boolean translations, boolean ranks, boolean columns) {

		try {

			// channel code cannot be null
			if (channelCode == null || channelCode.length() == 0) {
				return false;
			}

			// assign the code to the name field if it was left blank
			if (channelName == null || channelName.length() == 0) {
				channelName = channelCode;
			}

			// prepare the database that we are going to work on
			database.useDatabase(dbName);

			// channels flag states we need to add a record to the channels table
			if (channels) {
				String columnList	= "code, name, lon, lat, height";
				String variableList	= "?,?,?,?,?";
				
				if (translations) {
					columnList		= columnList + ",tid";
					variableList	= variableList + ",?";
				}
				
				ps = database.getPreparedStatement("INSERT INTO channels (" + columnList + ") VALUES (" + variableList + ")");
				
				ps.setString(1, channelCode);
				ps.setString(2, channelName);				
				if (Double.isNaN(lon))    { ps.setNull(3, java.sql.Types.DOUBLE); } else { ps.setDouble(3, lon);    }
				if (Double.isNaN(lat))    { ps.setNull(4, java.sql.Types.DOUBLE); } else { ps.setDouble(4, lat);    }
				if (Double.isNaN(height)) { ps.setNull(5, java.sql.Types.DOUBLE); } else { ps.setDouble(5, height); }
				if (translations)         { ps.setInt(6, tid); }
				ps.execute();
			}

			// look up the columns from the columns table and create the table
			if (columns) {
				
				List<Column> columnsList = defaultGetColumns(true, false);
				if (columnsList.size() > 0) {
	
					// prepare the channels table sql, PRIMARY KEY is defined below
					String sql = "CREATE TABLE " + channelCode + " (j2ksec DOUBLE";
	
					// loop. all sql columns in the db are of type double and allow for null values
					for (int i = 0; i < columnsList.size(); i++) {
						sql = sql + "," + columnsList.get(i).name + " DOUBLE";
					}
	
					// if this channel uses translations then the channel table needs to have a tid
					if (translations) {
						sql = sql + ",tid INT DEFAULT 1 NOT NULL";
					}
	
					// if this channel uses ranks then the channel table needs to have a rid
					if (ranks) {
						sql = sql + ",rid INT DEFAULT 1 NOT NULL,PRIMARY KEY(j2ksec,rid)";
	
					// when using ranks, the primary key is the combo of j2ksec and rid, otherwise, it's just the j2ksec
					} else {
						sql = sql + ",PRIMARY KEY(j2ksec)";
					}
	
					// place the closing parenthesis and execute the sql statement
					sql = sql + ",KEY index_j2ksec (j2ksec))";
					ps = database.getPreparedStatement(sql);
					ps.execute(sql);
				}
			}
			
			logger.log(Level.INFO, "SQLDataSource.defaultCreateChannel(" + channelCode + "," + lon + "," + lat + ") succeeded. (" + database.getDatabasePrefix() + "_" + dbName + ")");			
			return true;

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultCreateChannel(" + channelCode + "," + lon + "," + lat + ") failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return false;
	}

	/**
	 * Create entry in the channels table and creates a table for that channel
	 * @param channelCode	channel code
	 * @param channelName	channel name
	 * @param lon			longitude
	 * @param lat			latitude
	 * @param height		height
	 * @param tid			translation id
	 * @param azimuth		azimuth of the deformation source
	 * @param channels		
	 * @param translations
	 * @param ranks
	 * @param columns
	 * @params channelTypes
	 * @return true if successful
	 */	
	public boolean defaultCreateTiltChannel(Channel channel, int tid, double azimuth,
			boolean channels, boolean translations, boolean ranks, boolean columns) {
		
		try {
			defaultCreateChannel(channel, tid, channels, translations, ranks, columns);
			
			// get the newly created channel id
			Channel ch = defaultGetChannel(channel.getCode(), false);
			
			// update the channels table with the azimuth value
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("UPDATE channels SET azimuth = ? WHERE cid = ?");
			ps.setDouble(1, azimuth);
			ps.setInt(2, ch.getCID());
			ps.execute();
			
			return true;
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultCreateTiltChannel() failed.", e);
		}
		
		return false;
	}
	
	/**
	 * Updates the channels table with the specified translation id
	 * @param channelCode
	 * @param tid
	 * @return true if success
	 */
	public boolean defaultUpdateChannelTranslationID(String channelCode, int tid) {
		try {
			database.useDatabase(dbName);			
			ps = database.getPreparedStatement("UPDATE channels SET tid = ? WHERE code = ?");			
			ps.setInt(1, tid);
			ps.setString(2, channelCode);
			ps.execute();

			logger.log(Level.INFO, "SQLDataSource.defaultUpdateChannelTranslationID(" + channelCode + "," + tid + ") succeeded. (" + database.getDatabasePrefix() + "_" + dbName + ")");
			return true;

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultUpdateChannelTranslationID(" + channelCode + "," + tid + ") failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return false;
	}

	/**
	 * Create default translation table from values in the columns table
	 * 
	 * @return true if success
	 */
	public boolean defaultCreateTranslation() {
		try {
			database.useDatabase(dbName);
			
			// check if the translations table already exists
			boolean exists = false;
			rs = database.getPreparedStatement("SHOW TABLES LIKE 'translations'").executeQuery();
			if (rs.next()) {
				exists = true;
			}
			rs.close();

			if (exists) {
				return true;
				
			} else {

				List<Column> columns = defaultGetColumns(true, false);
				if (columns.size() > 0) {
	
					sql = "CREATE TABLE translations (tid INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255)";
					for (int i = 0; i < columns.size(); i++) {
						sql = sql + ",c" + columns.get(i).name + " DOUBLE DEFAULT 1,";
						sql = sql + " d" + columns.get(i).name + " DOUBLE DEFAULT 0 ";
					}
					sql = sql + ")";
	
					// the translations table has a default row inserted which will
					// be tid 1, which corresponds to the default tid in the channels table
					ps.execute(sql);
					ps.execute("INSERT INTO translations (name) VALUES ('DEFAULT')");
				}
	
				logger.log(Level.INFO, "SQLDataSource.defaultCreateTranslation() succeeded. (" + database.getDatabasePrefix() + "_" + dbName + ")");
				return true;
			}

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultCreateTranslation() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return false;
	}

	/**
	 * Insert column
	 * 
	 * @param column	Column return true if successful
	 */
	public boolean defaultInsertColumn(Column column) {
		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("INSERT IGNORE INTO columns (idx, name, description, unit, checked, active) VALUES (?,?,?,?,?,?)");
			ps.setInt(1, column.idx);
			ps.setString(2, column.name);
			ps.setString(3, column.description);
			ps.setString(4, column.unit);
			ps.setBoolean(5, column.checked);
			ps.setBoolean(6, column.active);
			ps.execute();
			
			logger.log(Level.INFO, "SQLDataSource.defaultInsertColumn(" + column.name + ") succeeded. (" + database.getDatabasePrefix() + "_" + dbName + ")");			
			return true;

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultInsertColumn(" + column.name + ") failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}
		return false;
	}

	/**
	 * Insert plot column
	 * 
	 * @param column	Column return true if successful
	 */
	public boolean defaultInsertMenuColumn(Column column) {
		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("INSERT IGNORE INTO columns_menu (idx, name, description, unit, checked, active) VALUES (?,?,?,?,?,?)");
			ps.setInt(1, column.idx);
			ps.setString(2, column.name);
			ps.setString(3, column.description);
			ps.setString(4, column.unit);
			ps.setBoolean(5, column.checked);
			ps.setBoolean(6, column.active);
			ps.execute();
			
			logger.log(Level.INFO, "SQLDataSource.defaultInsertPlotColumn(" + column.name + ") succeeded. (" + database.getDatabasePrefix() + "_" + dbName + ")");
			return true;

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultInsertPlotColumn(" + column.name + ") failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}
		return false;
	}

	/**
	 * Create new channel type
	 * 
	 * @param name	channel type display name
	 * @return last inserted id or -1 if unsuccessful
	 */
	public int defaultInsertChannelType(String name) {
		int result = -1;

		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("INSERT INTO channel_types (name) VALUES (?)");
			ps.setString(1, name);
			ps.execute();

			// get the id of the newly inserted channel type
			rs = database.getPreparedStatement("SELECT LAST_INSERT_ID()").executeQuery();
			if (rs.next()) {
				result = rs.getInt(1);
			}			
			rs.close();
			
			logger.log(Level.INFO, "SQLDataSource.defaultInsertChannelType(" + name + ") succeeded. (" + database.getDatabasePrefix() + "_" + dbName + ")");

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultInsertChannelType(" + name + ") failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return result;
	}
	
	/**
	 * Inserts rank into database.
	 * @param rank
	 * @return newly inserted rank.  null 
	 */
	public Rank defaultInsertRank(Rank rank) {
		return defaultInsertRank(rank.getName(), rank.getRank(), rank.getUserDefault());
	}

	/**
	 * Create new rank
	 * 
	 * @param name			rank display name
	 * @param rank			integer value of rank
	 * @param is_default	flag to set new rank as default
	 * @return Rank object using the specified 
	 */
	public Rank defaultInsertRank(String name, int rank, int is_default) {
		Rank result = null;
		int user_default = 0;

		try {
			
			int rid	= defaultGetRankID(rank);
			if (rid > 0) {
				return defaultGetRank(rid);
			}
			
			database.useDatabase(dbName);

			// if updating the default value then set all other default values
			// to 0 (there can only be one row set to default)
			if (is_default == 1) {
				ps = database.getPreparedStatement("UPDATE ranks set user_default = 0");
				ps.execute();
			}

			// create the new rank
			ps = database.getPreparedStatement("INSERT INTO ranks (name, rank, user_default) VALUES (?,?,?)");
			ps.setString(1, name);
			ps.setInt(2, rank);
			ps.setInt(3, user_default);
			ps.execute();

			// get the id of the newly inserted rank
			rs = database.getPreparedStatement("SELECT LAST_INSERT_ID()").executeQuery();
			if (rs.next()) {
				result = defaultGetRank(rs.getInt(1));
			}
			rs.close();
			
			logger.log(Level.INFO, "SQLDataSource.defaultInsertRank(" + name + "," + rank + "," + user_default + ") succeeded. (" + database.getDatabasePrefix() + "_" + dbName + ")");

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultInsertRank(" + name + "," + rank + "," + user_default + ") failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return result;
	}

	/**
	 * Inserts a translation in the translations table
	 * 
	 * @param channelCode	channel code
	 * @param gdm			generic data matrix containing the translations
	 * @return tid translation id of new translation.  -1 if failure
	 */
	public int defaultInsertTranslation(String channelCode, GenericDataMatrix gdm) {

		// default local variables
		int tid			= 1;
		String columns	= "";
		String values	= "";

		try {
			database.useDatabase(dbName);

			DoubleMatrix2D dm		= gdm.getData();
			String[] columnNames	= gdm.getColumnNames();

			// iterate through the generic data matrix to get a list of the values
			for (int i = 0; i < columnNames.length; i++) {
				columns	+= columnNames[i] + ",";
				values	+= dm.get(0, i) + ",";
			}
			columns += "name";
			values += "'" + channelCode + "'";

			// insert the translation into the database
			ps = database.getPreparedStatement("INSERT INTO translations (" + columns + ") VALUES (" + values + ")");
			ps.execute();
			tid = defaultGetTranslation(channelCode, gdm);
			
			logger.log(Level.INFO, "SQLDataSource.defaultInsertTranslation() succeeded. (" + database.getDatabasePrefix() + "_" + dbName + ")");

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultInsertTranslation() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return tid;
	}

	/**
	 * Get channel from database
	 * 
	 * @param cid			channel id
	 * @param channelTypes	if the channels table has channel types
	 * @return channel
	 */
	public Channel defaultGetChannel(int cid, boolean channelTypes) {
		Channel ch = null;
		String code, name;
		double lon, lat, height;
		int ctid = 0;

		try {
			database.useDatabase(dbName);
			
			sql	= "SELECT code, name, lon, lat, height ";
			if (channelTypes) {
				sql = sql + ",ctid ";
			}
			sql = sql + "FROM  channels ";
			sql = sql + "WHERE cid = ?";
			
			ps = database.getPreparedStatement(sql);
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			if (rs.next()) {
				code	= rs.getString(1);
				name	= rs.getString(2);
				lon		= rs.getDouble(3);
				if (rs.wasNull()) { lon	= Double.NaN; }
				lat		= rs.getDouble(4);
				if (rs.wasNull()) { lat	= Double.NaN; }
				height	= rs.getDouble(5);
				if (rs.wasNull()) { height	= Double.NaN; }
				if (channelTypes) {
					ctid	= rs.getInt(6);
				}
				
				ch	= new Channel(cid, code, name, lon, lat, height, ctid);
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannel(cid) failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return ch;
	}

	/**
	 * Get channel from database
	 * 
	 * @param code			channel code
	 * @param channelTypes	if the channels table has channel types
	 * @return channel
	 */
	public Channel defaultGetChannel(String code, boolean channelTypes) {
		Channel ch	= null;

		try {
			database.useDatabase(dbName);
			ps	= database.getPreparedStatement("SELECT cid FROM channels WHERE code = ? ");
			ps.setString(1, code);
			rs	= ps.executeQuery();
			if (rs.next()) {
				int cid = rs.getInt(1);
				ch = defaultGetChannel(cid, channelTypes);
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannel(code) failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return ch;
	}

	/**
	 * Get channels list from database
	 * 
	 * @param channelTypes	if the channels table has channel types
	 * @return List of Channels
	 */
	public List<Channel> defaultGetChannelsList(boolean channelTypes) {
		List<Channel> result = new ArrayList<Channel>();
		Channel ch;
		int cid, ctid;
		String code, name;
		double lon, lat, height;

		try {
			database.useDatabase(dbName);
			
			sql	= "SELECT cid, code, name, lon, lat, height ";
			if (channelTypes) {
				sql = sql + ",ctid ";
			}
			sql = sql + "FROM  channels ";
			sql = sql + "ORDER BY code ";
			
			rs = database.getPreparedStatement(sql).executeQuery();
			while (rs.next()) {
				
				cid		= rs.getInt(1);
				code	= rs.getString(2);
				name	= rs.getString(3);
				lon		= rs.getDouble(4);
				if (rs.wasNull()) { lon	= Double.NaN; }
				lat		= rs.getDouble(5);
				if (rs.wasNull()) { lat	= Double.NaN; }
				height	= rs.getDouble(6);
				if (rs.wasNull()) { height	= Double.NaN; }
				if (channelTypes) {
					ctid	= rs.getInt(7);
				} else {
					ctid	= 0;
				}				
				ch	= new Channel(cid, code, name, lon, lat, height, ctid);
				result.add(ch);
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannelsList() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return result;
	}

	/**
	 * Get channels list from database
	 * 
	 * @param channelTypes	if the channels table has channel types
	 * @return List of Strings with : separated values
	 */
	public List<String> defaultGetChannels(boolean channelTypes) {
		List<String> result = new ArrayList<String>();

		try {
			List<Channel> channelsList = defaultGetChannelsList(channelTypes);
			for (Channel channel : channelsList) {
				result.add(channel.toString());
			}

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannels() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return result;
	}

	/**
	 * Get channel types list in format "ctid:name" from database
	 * 
	 * @return List of Strings with : separated values
	 */
	public List<String> defaultGetChannelTypes() {
		List<String> result = new ArrayList<String>();

		try {
			database.useDatabase(dbName);
			rs = database.getPreparedStatement("SELECT ctid, name FROM channel_types ORDER BY name").executeQuery();
			while (rs.next()) {
				result.add(String.format("%d:%s", rs.getInt(1), rs.getString(2)));
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannelTypes() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return result;
	}
	
	/**
	 * Get rank from database
	 * @param rank	user defined rank
	 * @return rank, null if not found
	 */
	public Rank defaultGetRank(Rank rank) {
		return defaultGetRank(defaultGetRankID(rank.getRank()));
	}
	
	/**
	 * Get rank from database
	 * @param rid	rank id
	 * @return rank
	 */
	public Rank defaultGetRank(int rid) {
		Rank result	= null;
		
		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("SELECT rid, name, rank, user_default FROM ranks WHERE rid = ?");
			ps.setInt(1, rid);
			rs = ps.executeQuery();
			if (rs.next()) {
				result = new Rank(rs.getInt(1), rs.getString(2), rs.getInt(3), rs.getInt(4));
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetRank() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}
		
		return result;
	}
	
	/**
	 * Get rank id from database
	 * @param rank
	 * @return rank id
	 */
	public int defaultGetRankID(int rank) {
		int result	= -1;
		
		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("SELECT rid FROM ranks WHERE rank = ?");
			ps.setInt(1, rank);
			rs = ps.executeQuery();
			if (rs.next()) {
				result = rs.getInt(1);
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetRankID() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}
		
		return result;
	}

	/**
	 * Get ranks list in format "rid:name:rank:user_default" from database
	 * 
	 * @return List of Strings with : separated values
	 */
	public List<String> defaultGetRanks() {
		List<String> result = new ArrayList<String>();

		try {
			database.useDatabase(dbName);
			rs = database.getPreparedStatement("SELECT rid, name, rank, user_default FROM ranks ORDER BY rank").executeQuery();
			while (rs.next()) {
				result.add(String.format("%d:%s:%d:%d", rs.getInt(1), rs.getString(2), rs.getInt(3), rs.getInt(4)));
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetRanks() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return result;
	}

	/**
	 * Gets translation id from database using the parameters passed. Used to determine if the
	 * translation exists in the database for potentially inserting a new translation.
	 * 
	 * @param channelCode	channel code
	 * @param gdm			generic data matrix containing the translations
	 * @return tid translation id of the translation. -1 if not found.
	 */
	public int defaultGetTranslation(String channelCode, GenericDataMatrix gdm) {
		int result = 1;

		try {
			database.useDatabase(dbName);

			// iterate through the generic data matrix to get a list of the columns and their values
			DoubleMatrix2D dm		= gdm.getData();
			String[] columnNames	= gdm.getColumnNames();
			sql						= "";
			for (int i = 0; i < columnNames.length; i++) {
				sql += "AND " + columnNames[i] + " = " + dm.get(0, i) + " ";
			}

			// build and execute the query
			ps = database.getPreparedStatement("SELECT tid FROM translations WHERE name = ? " + sql);
			ps.setString(1, channelCode);
			rs = ps.executeQuery();
			if (rs.next()) {
				result = rs.getInt(1);
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetTranslation() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return result;
	}

	/**
	 * lookup translation id based on channel code
	 * 
	 * @param channelCode
	 * @return translation id, 1 if not found
	 */
	public int defaultGetChannelTranslationID(String channelCode) {
		int result = 1;

		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("SELECT tid FROM channels WHERE code = ?");
			ps.setString(1, channelCode);
			rs = ps.executeQuery();
			if (rs.next()) {
				result = rs.getInt(1);
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannelTranslationID() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return result;
	}
	
	/**
	 * Get List of columns from the database
	 * @return String List of columns
	 */
	public List<String> defaultGetMenuColumns(boolean menuColumns) {
		List<Column> columns = defaultGetColumns(false, menuColumns);
		List<String> columnsString = new ArrayList<String>();
		for (int i = 0; i < columns.size(); i++) {
			columnsString.add(columns.get(i).toString());
		}
		return columnsString;
	}

	/**
	 * Getter for columns
	 * @param allColumns	flag to retrieve only active columns from table
	 * @param menuColumns	flag to retrieve database columns or plottable columns
	 * @return List of Columns, ordered by index
	 */
	public List<Column> defaultGetColumns(boolean allColumns, boolean menuColumns) {

		Column column;
		List<Column> columns = new ArrayList<Column>();
		boolean checked, active;
		String tableName	= "";
		
		if (menuColumns) {
			tableName	= "columns_menu";
		} else {
			tableName	= "columns";
		}

		try {
			database.useDatabase(dbName);
			sql  = "SELECT idx, name, description, unit, checked, active ";
			sql += "FROM " + tableName + " ";
			if (!allColumns && !menuColumns) {
				sql += "WHERE active = 1 ";
			}
			sql += "ORDER BY idx, name";
			ps = database.getPreparedStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				if (rs.getInt(5) == 0) {
					checked = false;
				} else {
					checked = true;
				}
				if (rs.getInt(6) == 0) {
					active = false;
				} else {
					active = true;
				}
				column = new Column(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getString(4), checked, active);
				columns.add(column);
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetColumns() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return columns;
	}

	/**
	 * Get channel from database
	 * 
	 * @param colid			column id
	 * @return column, null if not found
	 */
	public Column defaultGetColumn(int colid) {
		Column col = null;
		int idx;
		String name, description, unit;
		boolean checked, active;

		try {
			database.useDatabase(dbName);
			
			sql	= "SELECT idx, name, description, unit, checked, active ";
			sql = sql + "FROM  columns ";
			sql = sql + "WHERE colid = ?";
			
			ps = database.getPreparedStatement(sql);
			ps.setInt(1, colid);
			rs = ps.executeQuery();
			if (rs.next()) {
				idx			= rs.getInt(1);
				name		= rs.getString(2);
				description	= rs.getString(3);
				unit		= rs.getString(4);
				if (rs.getInt(5) == 0) {
					checked = false;
				} else {
					checked = true;
				}
				if (rs.getInt(6) == 0) {
					active = false;
				} else {
					active = true;
				}
				col	= new Column(idx, name, description, unit, checked, active);
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetColumn(colid) failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return col;
	}

	/**
	 * Get channel from database
	 * 
	 * @param name			column name
	 * @return column
	 */
	public Column defaultGetColumn(String name) {
		Column col	= null;

		try {
			database.useDatabase(dbName);
			ps	= database.getPreparedStatement("SELECT colid FROM columns WHERE name = ? ");
			ps.setString(1, name);
			rs	= ps.executeQuery();
			if (rs.next()) {
				int colid = rs.getInt(1);
				col = defaultGetColumn(colid);
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannel(name) failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return col;
	}

	/**
	 * Get options list in format "idx:code:name" from database
	 * 
	 * @return List of Strings with : separated values
	 */
	public List<String> defaultGetOptions(String type) {
		List<String> result = new ArrayList<String>();

		try {
			database.useDatabase(dbName);
			sql	= "SELECT idx, code, name ";
			sql+= "FROM   options_" + type + " ";
			sql+= "ORDER BY idx";
			rs = database.getPreparedStatement(sql).executeQuery();
			while (rs.next()) {
				result.add(String.format("%d:%s:%s", rs.getInt(1), rs.getString(2), rs.getString(3)));
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetOptions() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return result;
	}

	/** 
	 * Gets the most recent timestamp in the database for the specified channel
	 * 
	 * @param channelCode
	 * @return most recent timestamp
	 */
	public synchronized Date defaultGetLastDataTime(String channelCode) {
		Date lastDataTime = null;

		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("SELECT max(j2ksec) FROM " + channelCode);
			rs = ps.executeQuery();
			if (rs.next()) {
				double result	= rs.getDouble(1);
				lastDataTime	= Util.j2KToDate(result);
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetLastDataTime() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return lastDataTime;
	}

	/**
	 * Get data from database
	 * 
	 * @param cid			channel id
	 * @param rid			rank id
	 * @param st			start time
	 * @param et			end time
	 * @param translations	if the database has translations
	 * @param ranks			if the database has ranks
	 * @return GenericDataMatrix containing the data
	 */
	public GenericDataMatrix defaultGetData(int cid, int rid, double st, double et, boolean translations, boolean ranks) {

		double[] dataRow;
		List<double[]> pts			= new ArrayList<double[]>();
		GenericDataMatrix result	= null;
		List<Column> columns		= new ArrayList<Column>();
		Column column;
		int columnsReturned = 0;
		double value;

		try {
			database.useDatabase(dbName);

			// look up the channel code from the channels table, which is the name of the table to query
			// channel types is false because at this point we don't care about that, just trying to get the channel name
			Channel channel	= defaultGetChannel(cid, false);
			columns			= defaultGetColumns(false, false);

			// if we are getting ranked data back, then we need to include the rid, otherwise, just add in a field for j2ksec
			if (ranks) {
				columnsReturned = columns.size() + 2;
			} else {
				columnsReturned = columns.size() + 1;
			}

			// SELECT sql
			sql = "SELECT a.j2ksec";
			
			if (ranks) {
				sql = sql + ", c.rid";
			}
			
			for (int i = 0; i < columns.size(); i++) {
				column = columns.get(i);
				if (translations) {
					sql = sql + "," + column.name + " * c" + column.name + " + d" + column.name + " as " + column.name + " ";
				} else {
					sql = sql + "," + column.name + " ";
				}
			}

			// FROM sql
			sql	= sql + "FROM " + channel.getCode() + " a ";
			if (translations) {
				sql = sql + "INNER JOIN translations b on a.tid = b.tid ";
			}
			if (ranks) {
				sql	= sql + "INNER JOIN ranks        c on a.rid = c.rid ";
			}

			// WHERE sql
			sql = sql + "WHERE j2ksec >= ? ";
			sql = sql + "AND   j2ksec <= ? ";
			
			// BEST POSSIBLE DATA query
			if (ranks && rid != 0) {
				sql = sql + "AND   c.rid  = ? ";
			} else if (ranks && rid == 0) {
				sql = sql + "AND   c.rank = (SELECT MAX(e.rank) " +
				                            "FROM   " + channel.getCode() + " d, ranks e " +
				                            "WHERE  d.rid = e.rid  " +
				                            "AND    a.j2ksec = d.j2ksec " +
				                            "AND    d.j2ksec >= ? " +
				                            "AND    d.j2ksec <= ? ) ";
			}
			sql = sql + "ORDER BY a.j2ksec ASC";

			ps = database.getPreparedStatement(sql);
			ps.setDouble(1, st);
			ps.setDouble(2, et);
			if (ranks && rid != 0) {
				ps.setInt(3, rid);
			} else {
				ps.setDouble(3, st);
				ps.setDouble(4, et);
			}
			rs = ps.executeQuery();
			
			// loop through each result and add to the list
			while (rs.next()) {
				
				// loop through each of the columns and convert to Double.NaN if it was null in the DB
				dataRow = new double[columnsReturned];
				for (int i = 0; i < columnsReturned; i++) {
					value	= rs.getDouble(i + 1);
					if (rs.wasNull()) { value = Double.NaN; }
					dataRow[i] = value;
				}
				pts.add(dataRow);
			}
			rs.close();

			if (pts.size() > 0) {
				result = new GenericDataMatrix(pts);
			}

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetData() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}
		
		return result;
	}

	/**
	 * Insert data
	 * 
	 * @param channelCode	table name
	 * @param gdm			2d matrix of data
	 * @param translations	if the database uses translations
	 * @param ranks			if the database uses ranks
	 * @param rid			rank id
	 */
	public void defaultInsertData(String channelCode, GenericDataMatrix gdm, boolean translations, boolean ranks, int rid) {

		double value;
		int tid						= 1;
		String[] columnNames		= gdm.getColumnNames();
		DoubleMatrix2D data			= gdm.getData();
		StringBuffer columnBuffer	= new StringBuffer();
		StringBuffer valuesBuffer	= new StringBuffer();
		String output, base;

		try {
			database.useDatabase(dbName);

			// build the columns string for a variable number of columns
			for (int i = 0; i < columnNames.length; i++) {
				if (i == 0) {
					columnBuffer.append(columnNames[i]);
					valuesBuffer.append("?");
				} else {
					columnBuffer.append("," + columnNames[i]);
					valuesBuffer.append(",?");
				}
			}

			// add in translation related information
			if (translations) {
				tid = defaultGetChannelTranslationID(channelCode);
				columnBuffer.append(",tid");
				valuesBuffer.append("," + tid);
			}

			// add in rank related information
			if (ranks) {
				columnBuffer.append(",rid");
				valuesBuffer.append("," + rid);
			}

			sql		= "REPLACE INTO " + channelCode + " (" + columnBuffer.toString() + ") VALUES (" + valuesBuffer.toString() + ")";
			base	= channelCode + "(";			
			
			ps = database.getPreparedStatement(sql);
			
			// loop through each of the rows and insert data
			for (int i = 0; i < gdm.rows(); i++) {				
				output = base;
				
				// loop through each of the columns and set it
				for (int j = 0; j < columnNames.length; j++) {
					
					// check for null values and use the correct setter function if so
					value	= data.getQuick(i, j);
					if (Double.isNaN(value)) {
						ps.setNull(j + 1, java.sql.Types.DOUBLE);
					} else {
						ps.setDouble(j + 1, value);
					}
					output = output + value + ",";
				}
				ps.execute();
				if (translations)	output += tid + ",";
				if (ranks)			output += rid + ",";
				logger.log(Level.INFO, "InsertData() " + database.getDatabasePrefix() + "_" + dbName + "." + output.substring(0, output.length() - 1) + ")");
			}

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultInsertData() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}
	}
	
	public void insertV2TiltData (String code, double j2ksec, double x, double y, double h, double b, double i, double g, double r) {
		try {
			
			// default some variables
			int tid = -1;
			int oid = -1;
			int eid = -1;
			
			// set the database
			database.useV2Database("tilt");
			
			// get the translation and offset
            ps = database.getPreparedStatement("SELECT curTrans, curOffset, curEnv FROM stations WHERE code=?");
            ps.setString(1, code);
            rs = ps.executeQuery();
            if (rs.next()) {
            	tid = rs.getInt(1);
            	oid = rs.getInt(2);
            	eid = rs.getInt(3);
            }
            rs.close();
            
            // lower case the code because that's how the table names are in the database
            code.toLowerCase();

            // create the tilt entry
            ps = database.getPreparedStatement("INSERT IGNORE INTO " + code + "tilt VALUES (?,?,?,?,?,?,?,?)");
			ps.setDouble(1, j2ksec);
			ps.setString(2, Util.j2KToDateString(j2ksec));
			if (Double.isNaN(x)) { ps.setNull(3, 8); } else { ps.setDouble(3, x); }
			if (Double.isNaN(y)) { ps.setNull(4, 8); } else { ps.setDouble(4, y); }
			if (Double.isNaN(g)) { ps.setNull(5, 8); } else { ps.setDouble(5, g); }
			ps.setDouble(6, tid);
			ps.setDouble(7, oid);
			ps.setDouble(8, 0);
			ps.execute();
			
			// create the environment entry
            ps = database.getPreparedStatement("INSERT IGNORE INTO " + code + "env VALUES (?,?,?,?,?,?,?,?)");
			ps.setDouble(1, j2ksec);
			ps.setString(2, Util.j2KToDateString(j2ksec));
			if (Double.isNaN(h)) { ps.setNull(3, 8); } else { ps.setDouble(3, h); }
			if (Double.isNaN(b)) { ps.setNull(4, 8); } else { ps.setDouble(4, b); }
			if (Double.isNaN(i)) { ps.setNull(5, 8); } else { ps.setDouble(5, i); }
			if (Double.isNaN(r)) { ps.setNull(6, 8); } else { ps.setDouble(6, r); }
			if (Double.isNaN(g)) { ps.setNull(7, 8); } else { ps.setDouble(7, g); }
			ps.setDouble(8, eid);
			ps.execute();
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.insertV2Data() failed.", e);
		}
	}

	public void insertV2StrainData(String code, double j2ksec, double dt01, double dt02, double barometer) {		
		try {
			
			// default some variables
			int tid = -1;
			int eid = -1;
            
            // lower case the code because that's how the table names are in the database
            code.toLowerCase();
			
			// set the database
			database.useV2Database("strain");
			
			// get the translation and offset
            ps = database.getPreparedStatement(
            		"SELECT curTrans, curEnvTrans FROM stations WHERE code=?");
            ps.setString(1, code);
            rs = ps.executeQuery();
            if (rs.next()) {
            	tid = rs.getInt(1);
            	eid = rs.getInt(2);
            }
            rs.close();

            // create the strain entry
            ps = database.getPreparedStatement("INSERT IGNORE INTO " + code + "strain (j2ksec, time, dt01, dt02, trans) VALUES (?,?,?,?,?)");
			ps.setDouble(1, j2ksec);
			ps.setString(2, Util.j2KToDateString(j2ksec));
			ps.setDouble(3, dt01);
			ps.setDouble(4, dt02);
			ps.setDouble(5, tid);
			ps.execute();
			
			// create the environment entry
            ps = database.getPreparedStatement("INSERT IGNORE INTO " + code + "env (j2ksec, time, bar ,trans) VALUES (?,?,?,?)");
			ps.setDouble(1, j2ksec);
			ps.setString(2, Util.j2KToDateString(j2ksec));
			ps.setDouble(3, barometer);
			ps.setDouble(4, eid);
			ps.execute();
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.insertV2StrainData() failed.", e);
		}		
	}
	
	public void insertV2GasData(int sid, double t, double co2) {		
		try {
			
			// set the database
			database.useV2Database("gas");

            // create the tilt entry
			ps = database.getPreparedStatement("INSERT IGNORE INTO co2 VALUES (?,?,?,?)");
			ps.setDouble(1, t);
			ps.setInt(2, sid);
			ps.setString(3, Util.j2KToDateString(t));
			ps.setDouble(4, co2);
			ps.execute();
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.insertV2GasData() failed.", e);
		}		
	}
}