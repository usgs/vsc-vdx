package gov.usgs.vdx.data;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.util.UtilException;
import gov.usgs.vdx.client.VDXClient.DownsamplingType;
import gov.usgs.vdx.data.MetaDatum;
import gov.usgs.vdx.data.SuppDatum;
import gov.usgs.vdx.db.VDXDatabase;
import gov.usgs.vdx.server.RequestResult;
import gov.usgs.vdx.server.TextResult;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.TimeZone;

import cern.colt.matrix.DoubleMatrix2D;

/**
 * SQL data source. 
 * Store reference to VDX database and provide methods to init default database structure.
 * 
 * TODO: use Statements for low rate queries.
 * 
 * @author Dan Cervelli, Loren Antolik
 */
abstract public class SQLDataSource implements DataSource {
	
	protected VDXDatabase database;
	protected String vdxName;
	protected String dbName;
	protected static Logger logger = Logger.getLogger("gov.usgs.vdx.data.SQLDataSource");

	protected Statement st;
	protected PreparedStatement ps;
	protected ResultSet rs;
	protected String sql;
	private int maxrows = 0;
	
	/**
	 * Initialize the data source.  Concrete realization see in the inherited classes
	 * @param params config file
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
	 * Disconnect from database. Concrete realization see in the inherited classes
	 */
	abstract public void disconnect();
	
	/**
	 * Getter for maxrows
	 * @return maxrows
	 */
	public int getMaxRows(){
		return maxrows;
	}
	
	/**
	 * Setter for maxrows
	 * @param maxrows limit on number of rows to retrieve
	protected void setMaxRows(int maxrows){
		this.maxrows = maxrows;
	}
	
	/**
	 * Get size of result set
	 * @param rs ResultSet to query 
	 * @return Count of records in given ResultSet
	 * @throws SQLException
	 */
	public static int getResultSetSize(ResultSet rs) throws SQLException {
		int size =0;
		int currentRow = rs.getRow();
		if (rs != null){  
		   rs.beforeFirst();  
		   rs.last();  
		   size = rs.getRow();
		   if(currentRow==0){
			   rs.beforeFirst();   
		   } else {
			   rs.absolute(currentRow);
		   }
		}
		return size;   
	}
	
	/**
	 * Get error result
	 * @param errMessage message to pack
	 * @return TextResult which contains error message
	 */
	public static RequestResult getErrorResult(String errMessage){
		List<String> text = new ArrayList<String>();
		text.add(errMessage);
		TextResult result = new TextResult(text);
		result.setError(true);
		return result;
	}
	
	/**
	 * Get SQL for downsampling
	 * @param sql Query to compress
	 * @param ds Type of compressing: currently decimate/mean by time interval/none
	 * @param dsInt time interval to average values, in seconds
	 * @return sql that get only subset of records from original sql
	 * @throws UtilException in case of unknown downsampling type
	 */
	public static String getDownsamplingSQL(String sql, String time_column, DownsamplingType ds, int dsInt) throws UtilException{
		if(!ds.equals(DownsamplingType.NONE) && dsInt<=1)
			throw new UtilException("Downsampling interval should be more than 1");
		if(ds.equals(DownsamplingType.NONE))
			return sql;
		else if(ds.equals(DownsamplingType.DECIMATE))
			return "SELECT * FROM(SELECT fullQuery.*, @row := @row+1 AS rownum FROM (" + sql + ") fullQuery, (SELECT @row:=0) r) ranked WHERE rownum % " + dsInt + " = 1";
		else if (ds.equals(DownsamplingType.MEAN)){
			String sql_select_clause = sql.substring(6, sql.toUpperCase().indexOf("FROM")-1);
			String sql_from_where_clause = sql.substring(sql.toUpperCase().indexOf("FROM")-1, sql.toUpperCase().lastIndexOf("ORDER BY")-1);
			String[] columns = sql_select_clause.split(",");
			String avg_sql = "SELECT ";
			for(String column: columns){
				String groupFunction = "AVG";
				String[] column_parts = column.trim().split("\\sas\\s");
				if(column_parts[0].equals(time_column)){
					groupFunction = "MIN";
				}
				if(column_parts.length>1){
					avg_sql += groupFunction + "("+column_parts[0]+") as " + column_parts[1] + ", ";
				} else {
					avg_sql += groupFunction + "("+column_parts[0]+"), ";
				}
			}
			avg_sql += "(((" + time_column + ") - ?) DIV ?) intNum ";
			avg_sql += sql_from_where_clause;
			avg_sql += " GROUP BY intNum";
			return avg_sql;
		}
		else
			throw new UtilException("Unknown downsampling type: " + ds);
	}
	
	/**
	 * Insert data.  Concrete realization see in the inherited classes
	 * @return true if success
	 */
	// abstract public void insertData(String channelCode, GenericDataMatrix gdm, boolean translations, boolean ranks, int rid);
	
	/**
	 * Initialize Data Source
	 * 
	 * @param params config file
	 */
	public void defaultInitialize(ConfigFile params) {
		
		// common database connection parameters
		String driver	= params.getString("vdx.driver");
		String url		= params.getString("vdx.url");
		String prefix	= params.getString("vdx.prefix");
		database		= new VDXDatabase(driver, url, prefix);
		vdxName			= params.getString("vdx.name");
		// dbName is an additional parameter that VDX classes uses, unlike Winston or Earthworm
		dbName			= vdxName + "$" + getType();
		maxrows			= Util.stringToInt(params.getString("maxrows"), 0); 
		logger.log(Level.INFO, "SQLDataSource.defaultInitialize(" + database.getDatabasePrefix() + "_" + dbName + ") succeeded.");
	}

	/**
	 * Close database connection
	 */
	public void defaultDisconnect() {
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
	 * @param menuColumns   flag to retrieve database columns or plottable columns
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
					+ "unit VARCHAR(255), checked TINYINT, active TINYINT, bypassmanipulations TINYINT)");
			}

			if (menuColumns) {
				ps.execute("CREATE TABLE columns_menu (colid INT PRIMARY KEY AUTO_INCREMENT, "
					+ "idx INT, name VARCHAR(255) UNIQUE, description VARCHAR(255), "
					+ "unit VARCHAR(255), checked TINYINT, active TINYINT, bypassmanipulations TINYINT)");
			}

			// the usage of ranks does not depend on there being a channels table
			if (ranks) {
				ps.execute("CREATE TABLE ranks (rid INT PRIMARY KEY AUTO_INCREMENT,"
					+ "name VARCHAR(24) UNIQUE, rank INT(10) UNSIGNED DEFAULT 0 NOT NULL, user_default TINYINT(1) DEFAULT 0 NOT NULL)");
			}
			
			ps.execute( "CREATE TABLE supp_data (sdid INT NOT NULL AUTO_INCREMENT, st DOUBLE NOT NULL, et DOUBLE, sdtypeid INT NOT NULL, "
					+ "sd_short VARCHAR(90) NOT NULL, sd TEXT NOT NULL, PRIMARY KEY (sdid))" );
			
			ps.execute( "CREATE TABLE supp_data_type (sdtypeid INT NOT NULL AUTO_INCREMENT, supp_data_type VARCHAR(20), "
					+ "supp_color VARCHAR(6) NOT NULL, draw_line TINYINT, PRIMARY KEY (sdtypeid), UNIQUE KEY (supp_data_type) )" );
			
			sql = "CREATE TABLE supp_data_xref ( sdid INT NOT NULL, cid INT NOT NULL, colid INT NOT NULL, ";
			String key = "UNIQUE KEY (sdid,cid,colid";
			if ( ranks ) {
				sql = sql + "rid INT NOT NULL, ";
				key = key + ",rid";
			}
			ps.execute( sql + key + "))" ); 

			sql = "CREATE TABLE channelmetadata ( cmid INT NOT NULL AUTO_INCREMENT, cid INT NOT NULL, colid INT NOT NULL, ";
			if ( ranks )
				sql = sql + "rid INT NOT NULL, ";
			sql = sql + "name VARCHAR(20) NOT NULL, value TEXT NOT NULL, UNIQUE KEY (cmid,cid,colid";
			if ( ranks )
				sql = sql + ",rid";
			ps.execute( sql + "))" );

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
	 * @return channel name
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
	 * @return true if successful
	 */	
	public boolean defaultCreateTiltChannel(Channel channel, int tid, double azimuth, boolean channels, boolean translations, boolean ranks, boolean columns) {
		try {
			defaultCreateChannel(channel, tid, channels, translations, ranks, columns);
			
			// get the newly created channel id
			Channel ch = defaultGetChannel(channel.getCode(), false);
			
			// update the channels table with the azimuth value
			String azimuth_column_name = null;
			if(dbName.toLowerCase().contains("tensorstrain")){
				azimuth_column_name = "natural_azimuth";
			} else {
				azimuth_column_name = "azimuth";				
			}
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("UPDATE channels SET " + azimuth_column_name + "  = ? WHERE cid = ?");
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
	 * @param column	Column 
	 * @return true if successful
	 */
	public boolean defaultInsertColumn(Column column) {
		try {
			database.useDatabase(dbName);
			ps = database.getPreparedStatement("INSERT IGNORE INTO columns (idx, name, description, unit, checked, active, bypassmanipulations) VALUES (?,?,?,?,?,?,?)");
			ps.setInt(1, column.idx);
			ps.setString(2, column.name);
			ps.setString(3, column.description);
			ps.setString(4, column.unit);
			ps.setBoolean(5, column.checked);
			ps.setBoolean(6, column.active);
			ps.setBoolean(7, column.bypassmanipulations);
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
			ps = database.getPreparedStatement("INSERT IGNORE INTO columns_menu (idx, name, description, unit, checked, active, bypassmanipulations) VALUES (?,?,?,?,?,?,?)");
			ps.setInt(1, column.idx);
			ps.setString(2, column.name);
			ps.setString(3, column.description);
			ps.setString(4, column.unit);
			ps.setBoolean(5, column.checked);
			ps.setBoolean(6, column.active);
			ps.setBoolean(7, column.bypassmanipulations);
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
		double lon, lat, height, azimuth;

		try {
			database.useDatabase(dbName);
			
			sql	= "SELECT cid, code, name, lon, lat, height ";
			if(dbName.toLowerCase().contains("tensorstrain")){
				sql = sql + ", natural_azimuth ";
			} else if(dbName.toLowerCase().contains("tilt")){
				sql = sql + ", azimuth ";
			} else {
				sql = sql + ", 0 ";
			}
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
				azimuth	= rs.getDouble(7);
				if (rs.wasNull()) { azimuth	= Double.NaN; }
				if (channelTypes) {
					ctid	= rs.getInt(8);
				} else {
					ctid	= 0;
				}				
				ch	= new Channel(cid, code, name, lon, lat, height, azimuth, ctid);
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
	 * param menuColumns flag to retrieve database columns or plottable columns
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
		boolean checked, active, bypassmanipulations;
		String tableName	= "";
		
		if (menuColumns) {
			tableName	= "columns_menu";
		} else {
			tableName	= "columns";
		}

		try {
			database.useDatabase(dbName);
			sql  = "SELECT idx, name, description, unit, checked, active, bypassmanipulations ";
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
				if (rs.getInt(7) == 0) {
					bypassmanipulations = false;
				} else {
					bypassmanipulations = true;
				}
				column = new Column(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getString(4), checked, active, bypassmanipulations);
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
		boolean checked, active, bypassmanipulations;

		try {
			database.useDatabase(dbName);
			
			sql	= "SELECT idx, name, description, unit, checked, active, bypassmanipulations ";
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
				if (rs.getInt(7) == 0) {
					bypassmanipulations = false;
				} else {
					bypassmanipulations = true;
				}
				col	= new Column(idx, name, description, unit, checked, active, bypassmanipulations);
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
	 * @param type suffix of table name
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
	 * @param maxrows       limit on number of rows returned
	 * @param ds            Downsampling type
	 * @param dsInt         argument for downsampling
	 * @return GenericDataMatrix containing the data
	 * @throws UtilException
	 */
	public GenericDataMatrix defaultGetData(int cid, int rid, double st, double et, boolean translations, boolean ranks, int maxrows, DownsamplingType ds, int dsInt) throws UtilException {

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
			sql = "SELECT j2ksec";
			
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
			try{
				sql = getDownsamplingSQL(sql, "j2ksec", ds, dsInt);
			} catch (UtilException e){
				throw new UtilException("Can't downsample dataset: " + e.getMessage());
			}
			if(maxrows !=0){
				sql += " LIMIT " + (maxrows+1);
			}

			ps = database.getPreparedStatement(sql);
			if(ds.equals(DownsamplingType.MEAN)){
				ps.setDouble(1, st);
				ps.setInt(2, dsInt);
				ps.setDouble(3, st);
				ps.setDouble(4, et);
				if (ranks && rid != 0) {
					ps.setInt(5, rid);
				} else {
					ps.setDouble(5, st);
					ps.setDouble(6, et);
				}
			} else {
				ps.setDouble(1, st);
				ps.setDouble(2, et);
				if (ranks && rid != 0) {
					ps.setInt(3, rid);
				} else {
					ps.setDouble(3, st);
					ps.setDouble(4, et);
				}
			}
			rs = ps.executeQuery();
			
			if(maxrows !=0 && getResultSetSize(rs)> maxrows){ 
				throw new UtilException("Configured row count (" + maxrows + "rows) for source '" + dbName + "' exceeded. Please use downsampling.");
			}
			// loop through each result and add to the list
			while (rs.next()) {
				
				// loop through each of the columns and convert to Double.NaN if it was null in the DB
				dataRow = new double[columnsReturned];
				for (int i = 0; i < columnsReturned; i++) {
					dataRow[i] = getDoubleNullCheck(rs, i+1);
				}
				pts.add(dataRow);
			}
			rs.close();

			if (pts.size() > 0) {
				result = new GenericDataMatrix(pts);
			}

		} catch (SQLException e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetData() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}
		
		return result;
	}
	
	/**
	 * Retrieves the value of the designated column in the current row
     * of <code>ResultSet</code> object as
     * a <code>double</code> in the Java programming language
	 * @param rs result set to extract data
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return column value; not like ResultSet.getData(), if the value is SQL <code>NULL</code>, the
     * value returned is <code>Double.NaN</code>
	 * @throws SQLException
	 */
	
	public double getDoubleNullCheck(ResultSet rs, int columnIndex) throws SQLException{
		double value	= rs.getDouble(columnIndex);
		if (rs.wasNull()) { value = Double.NaN; }
		return  value;
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
	
	/**
	 * Insert Tilt data into V2 database
	 *
	 * @param code
	 * @param j2ksec time
	 * @param x
	 * @param y
	 * @param h
	 * @param b
	 * @param i
	 * @param g
	 * @param r
	 */
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

	/**
	 * Insert Strain data into V2 database
	 *
	 * @param code
	 * @param j2ksec time
	 * @param dt01
	 * @param dt02
	 * @param trans
	 */
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
	
	/**
	 * Insert Gas data into V2 database
	 *
	 * @param sid station ID
	 * @param t time
	 * @param co2 co2 value
	 */
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
	
	/**
	 * Insert a piece of metadata
	 * 
	 * @param md the MetaDatum to be added
	 */
	public void insertMetaDatum( MetaDatum md ) {
		try {
			database.useDatabase(dbName);

			sql		= "INSERT INTO channelmetadata (cid,colid,rid,name,value) VALUES (" + md.cid + "," + md.colid + "," + md.rid + ",\"" + md.name + "\",\"" + md.value + "\");";

			ps = database.getPreparedStatement(sql);
			
			ps.execute();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.insertMetaDatum() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}
	}
	
	/**
	 * Update a piece of metadata
	 * 
	 * @param md the MetaDatum to be updated
	 */
	public void updateMetaDatum( MetaDatum md ) {
		try {
			database.useDatabase(dbName);

			sql		= "UPDATE channelmetadata SET cid='" + md.cid + "', colid='" + md.colid +
				"', rid='" + md.rid + "', name='" + md.name + "', value='" + md.value +
				"' WHERE cmid='" + md.cmid + "'";

			ps = database.getPreparedStatement(sql);
			
			ps.execute();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.updateMetaDatum() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}
	}
	
	/**
	 * Retrieve a piece of metadata
	 * 
	 * @param cmid the ID of the metadata to retrieve
	 * @return MetaDatum the desired metadata (null if not found)
	 */
	public MetaDatum getMetaDatum( int cmid ) {
		try {
			database.useDatabase(dbName);
			MetaDatum md = null;
			sql = "SELECT * FROM channelmetadata WHERE cmid = " + cmid;
			ps = database.getPreparedStatement( sql );
			rs	= ps.executeQuery();
			if (rs.next()) {
				md = new MetaDatum();
				md.cmid    = rs.getInt(1);
				md.cid     = rs.getInt(2);
				md.colid   = rs.getInt(3);
				md.rid     = rs.getInt(4);
				md.name    = rs.getString(5);
				md.value   = rs.getString(6);
				md.chName  = rs.getString(7);
				md.colName = rs.getString(8);
				md.rkName  = rs.getString(9);
			}
			rs.close();
			return md;
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.getMetaDatum() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
			return null;
		}
	}	

	/**
	 * Retrieve a collection of metadata
	 * 
	 * @param md the pattern to match (integers < 0 & null strings are ignored)
	 * @param cm = "is the name of the columns table coulmns_menu?"
	 * @return List<MetaDatum> the desired metadata (null if an error occurred)
	 */
	public List<MetaDatum> getMatchingMetaData( MetaDatum md, boolean cm ) {
		try {
			database.useDatabase(dbName);
			sql = "SELECT MD.*, CH.code, COL.name, RK.name FROM channelmetadata as MD, channels as CH, columns" + (cm ? "_menu" : "") + " as COL, ranks as RK "; // channelmetadata";
			String where = "WHERE MD.cid=CH.cid AND MD.colid=COL.colid AND MD.rid=RK.rid";
			
			if ( md.chName != null )
				where = where + " AND CH.code='" + md.chName + "'";
			else if ( md.cid >= 0 )
				where = where + " AND MD.cid=" + md.cid;
			if ( md.colName != null )
				where = where + " AND COL.name='" + md.colName + "'";
			else if ( md.colid >= 0 )
				where = where + " AND MD.colid=" + md.colid;
			if ( md.rkName != null )
				where = where + " AND RK.name='" + md.rkName + "'";
			else if ( md.rid >= 0 )
				where = where + " AND MD.rid=" + md.rid;
			if ( md.name != null )
				where = where + " AND MD.name=" + md.name;
			if ( md.value != null )
				where = where + " AND MD.value=" + md.value;
				
			logger.info( "SQL: " + sql + where );
			ps = database.getPreparedStatement( sql + where );
			rs	= ps.executeQuery();
			List<MetaDatum> result = new ArrayList<MetaDatum>();
			while (rs.next()) {
				md = new MetaDatum();
				md.cmid    = rs.getInt(1);
				md.cid     = rs.getInt(2);
				md.colid   = rs.getInt(3);
				md.rid     = rs.getInt(4);
				md.name    = rs.getString(5);
				md.value   = rs.getString(6);
				md.chName  = rs.getString(7);
				md.colName = rs.getString(8);
				md.rkName  = rs.getString(9);
				result.add( md );
			}
			rs.close();
			return result;
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.getMatchingMetaData() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
			return null;
		}
	}

	/**
	 * Process a getData request for metadata from this datasource
	 * 
	 * @param params parameters for this request
	 * @param cm = "is the name of the columns table coulmns_menu?"
	 * @return RequestResult the desired meta data (null if an error occurred)
	 */
	protected RequestResult getMetaData(Map<String, String> params, boolean cm) {
		String arg = params.get("byID");
		List<MetaDatum> data = null;
		MetaDatum md_s;
		if ( arg != null && arg.equals("true") ) {
			int cid			= Integer.parseInt(params.get("ch"));
			arg = params.get("col");
			int colid;
			if ( arg==null || arg=="" )
				colid = -1;
			else
				colid = Integer.parseInt(arg);
			arg = params.get("rk");
			int rid;
			if ( arg==null || arg=="" )
				rid = -1;
			else
				rid = Integer.parseInt(arg);
			md_s = new MetaDatum( cid, colid, rid );
		} else {
			String chName   = params.get("ch");
			String colName  = params.get("col");
			String rkName   = params.get("rk");
			md_s = new MetaDatum( chName, colName, rkName );
		}
		data = getMatchingMetaData( md_s, cm );
		if (data != null) {
			List<String> result = new ArrayList<String>();
			for ( MetaDatum md: data )
				result.add(String.format("%d,%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"", 
					md.cmid, md.cid, md.colid, md.rid, md.name, md.value, md.chName, md.colName, md.rkName ));
			return new TextResult(result);
		}
		return null;
	}

	/**
	 * Retrieve a collection of supplementary data
	 * 
	 * @param sd the pattern to match (integers < 0 & null strings are ignored)
	 * @param cm = "is the name of the columns table coulmns_menu?"
	 * @return List<SuppDatum> the desired supplementary data (null if an error occurred)
	 */
	public List<SuppDatum> getMatchingSuppData( SuppDatum sd, boolean cm ) {
		try {
			database.useDatabase(dbName);
			sql = "SELECT SD.*, CH.code, COL.name, RK.name, ST.supp_data_type, ST.supp_color, SX.cid, SX.colid, SX.rid, ST.draw_line " +
				"FROM supp_data as SD, channels as CH, columns" + (cm ? "_menu" : "") + " as COL, ranks as RK, supp_data_type as ST, supp_data_xref as SX "; // channelmetadata";
			String where = "WHERE SD.et >= " + sd.st + " AND SD.st <= " + sd.et + " AND SD.sdid=SX.sdid AND SD.sdtypeid=ST.sdtypeid AND SX.cid=CH.cid AND SX.colid=COL.colid AND SX.rid=RK.rid";
			
			if ( sd.chName != null )
				if ( sd.cid < 0 )
					where = where + " AND CH.code='" + sd.chName + "'";
				else
					where = where + " AND CH.cid IN (" + sd.chName + ")";
			else if ( sd.cid >= 0 )
				where = where + " AND SX.cid=" + sd.cid;
				
			if ( sd.colName != null )
				if ( sd.colid < 0 )
					where = where + " AND COL.name='" + sd.colName + "'";
				else
					where = where + " AND COL.colid IN (" + sd.colName + ")";
			else if ( sd.colid >= 0 )
				where = where + " AND SX.colid=" + sd.colid;
				
			if ( sd.rkName != null )
				if ( sd.rid < 0 )
					where = where + " AND RK.name='" + sd.rkName + "'";
				else
					where = where + " AND RK.rid IN (" + sd.rkName + ")";
			else if ( sd.rid >= 0 )
				where = where + " AND SX.rid=" + sd.rid;
				
			if ( sd.name != null )
				where = where + " AND SD.sd_short=" + sd.name;	
			if ( sd.value != null )
				where = where + " AND SD.sd=" + sd.value;
			
			String type_filter = null;
			if ( sd.typeName != null )
				if ( sd.typeName.length() == 0 )
					;
				else if ( sd.tid == -1 )
					type_filter = "ST.supp_data_type='" + sd.typeName + "'";
				else
					type_filter = "ST.sdtypeid IN (" + sd.typeName + ")";
			else if ( sd.tid >= 0 )
				type_filter = "SD.sdtypeid=" + sd.tid;

			if ( sd.dl == -1 ) {
				if ( type_filter != null )
					where = where + " AND " + type_filter;
			} else if ( sd.dl < 2 ) {
				if ( type_filter != null )
					where = where + " AND " + type_filter;
				where =  where + " AND ST.dl='" + sd.dl;
			} else if ( type_filter != null ) 
				where = where + " AND (" + type_filter + " OR ST.draw_line='0')";
			else
				where = where + " AND ST.draw_line='0'";
					
				
			//logger.info( "SQL: " + sql + where );
			ps = database.getPreparedStatement( sql + where );
			rs	= ps.executeQuery();
			List<SuppDatum> result = new ArrayList<SuppDatum>();
			while (rs.next()) {
				result.add( new SuppDatum(rs) );
			}
			rs.close();
			return result;
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.getMatchingSuppData() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
			return null;
		}
	}
	
	/**
	 * Insert a piece of supplemental data
	 * 
	 * @param sd the SuppDatum to be added
	 * @return ID of the record, -ID if already present, 0 if failed
	 */
	public int insertSuppDatum( SuppDatum sd ) {
		try {
			database.useDatabase(dbName);

			sql		= "INSERT INTO supp_data (sdtypeid,st,et,sd_short,sd) VALUES (" + 
				sd.tid + "," + sd.st + "," + sd.et + ",\"" + sd.name  + "\",\"" + sd.value + 
				"\")";

			ps = database.getPreparedStatement(sql);
			
			ps.execute();
			
			rs = ps.getGeneratedKeys(); 

			rs.next();
			
			return rs.getInt(1); 
		} catch (SQLException e) {
			if ( !e.getSQLState().equals("23000") ) {
				logger.log(Level.SEVERE, "SQLDataSource.insertSuppDatum() failed. (" + 
					database.getDatabasePrefix() + "_" + dbName + ")", e);
				return 0;
			}
		}
		try {
			sql = "SELECT sdid FROM supp_data WHERE sdtypeid=" + sd.tid + " AND st=" + sd.st +
				" AND et=" + sd.et + " AND sd_short='" + sd.name + "'";
			ps = database.getPreparedStatement(sql);
			
			int sdid = 0;

			rs = ps.executeQuery();

			rs.next();

			return -rs.getInt(1);
		} catch ( SQLException e2 ) {
			logger.log(Level.SEVERE, "SQLDataSource.insertSuppDatum() failed. (" + 
				database.getDatabasePrefix() + "_" + dbName + ")", e2 );
			return 0;
		}
	}
	
	/**
	 * Update a piece of supplemental data
	 * 
	 * @param sd the SuppDatum to be added
	 * @return ID of the record, 0 if failed
	 */
	public int updateSuppDatum( SuppDatum sd ) {
		try {
			database.useDatabase(dbName);

			sql		= "UPDATE supp_data SET sdtypeid='" + sd.tid + "',st='" + sd.st + "',et='" + sd.et + 
				"',sd_short='" + sd.name + "',sd='" + sd.value + "' WHERE sdid='" + sd.sdid + "'";
				
			ps = database.getPreparedStatement(sql);
			
			ps.execute();
			
			return sd.sdid;
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "SQLDataSource.updateSuppDatum() failed. (" + 
				database.getDatabasePrefix() + "_" + dbName + ")", e);
			return 0;
		}
	}	
								
	/**
	 * Insert a supplemental data xref
	 * 
	 * @param sdx the SuppDatum xref to be added
	 * @return true if successful, false otherwise
	 */
	public boolean insertSuppDatumXref( SuppDatum sd ) {
		try {
			database.useDatabase(dbName);
			sql		= "INSERT INTO supp_data_xref (sdid, cid, colid, rid) VALUES (" +
				sd.sdid + "," + sd.cid + "," + sd.colid + "," + sd.rid + ");";

			ps = database.getPreparedStatement(sql);
		
			ps.execute();
		} catch (SQLException e) {
			if ( !e.getSQLState().equals("23000") ) {
				logger.log(Level.SEVERE, "SQLDataSource.insertSuppDatumXref() failed. (" + 
					database.getDatabasePrefix() + "_" + dbName + ")", e);
				return false;
			}
			logger.info( "SQLDataSource.insertSuppDatumXref: SDID " + 
				sd.sdid + " xref already exists for given parameters" );
		}
		return true;
	}

	/**
	 * Insert a supplemental datatype
	 * 
	 * @param sd the datatype to be added
	 * @return ID of the datatype, -ID if already present, 0 if failed
	 */
	public int insertSuppDataType( SuppDatum sd ) {
		try {
			database.useDatabase(dbName);

			sql		= "INSERT INTO supp_data_type (supp_data_type,supp_color,draw_line) VALUES (" + 
				"\"" + sd.typeName + "\",\"" + sd.color + "\"," + sd.dl + ");";

			ps = database.getPreparedStatement(sql);
			
			ps.execute();
			
			rs = ps.getGeneratedKeys(); 

			rs.next();
			
			return rs.getInt(1); 
		} catch (SQLException e) {
			if ( !e.getSQLState().equals("23000") ) {
				logger.log(Level.SEVERE, "SQLDataSource.insertSuppDataType() failed. (" + 
					database.getDatabasePrefix() + "_" + dbName + ")", e);
				return 0;
			}
		}
		try {
			sql = "SELECT sdid FROM supp_data WHERE sdtypeid=" + sd.tid + " AND st=" + sd.st +
				" AND et=" + sd.et + " AND sd_short='" + sd.name + "'";
			ps = database.getPreparedStatement(sql);
			rs = ps.executeQuery();
			rs.next();
			return -rs.getInt(1);
			
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "SQLDataSource.insertSuppDataType() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
			return 0;
		}
	}

	/**
	 * Process a getData request for supplementary data from this datasource
	 * 
	 * @param params parameters for this request
	 * @param cm = "is the name of the columns table coulmns_menu?"
	 * @return RequestResult the desired supplementary data (null if an error occurred)
	 */
	protected RequestResult getSuppData(Map<String, String> params, boolean cm) {
		double st, et;
		String arg = null;
		List<SuppDatum> data = null;
		SuppDatum sd_s;
		
		String tz = params.get("tz");
		if ( tz==null || tz.equals("") )
			tz = "UTC";
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		df.setTimeZone(TimeZone.getTimeZone(tz));
		
		try {
			arg = params.get("st");
			st =  Util.dateToJ2K(df.parse(arg));
			arg = params.get("et");
			if ( arg==null || arg.equals("") )
				et = Double.MAX_VALUE;
			else
				et = Util.dateToJ2K(df.parse(arg));
		} catch (Exception e) {
			return getErrorResult("Illegal time string: " + arg + ", " + e);
		}

		arg = params.get("byID");
		if ( arg != null && arg.equals("true") ) {
			//arg = params.get("et");
			//if ( arg==null || arg.equals("") )
			//	et = Double.MAX_VALUE;
			//else
			//	et = Double.parseDouble(arg);
			sd_s = new SuppDatum( st, et, -1, -1, -1, -1 );
			String[] args = {"ch","col","rk","type"};
			for ( int i = 0; i<4; i++ ) {
				arg = params.get( args[i] );
				if ( arg==null || arg.equals("") )
					args[i] = null;
				else 
					args[i] = arg;
			}
			sd_s.chName = args[0];
			if ( sd_s.chName != null )
				sd_s.cid = 0;
			sd_s.colName = args[1];
			if ( sd_s.colName != null )
				sd_s.colid = 0;
			sd_s.rkName = args[2];
			if ( sd_s.rkName != null )
				sd_s.rid = 0;
			sd_s.typeName = args[3];
			if ( sd_s.typeName != null )
				sd_s.tid = 0;
		} else {
			String chName   = params.get("ch");
			String colName  = params.get("col");
			String rkName   = params.get("rk");
			String typeName = params.get("type");
			sd_s = new SuppDatum( st, et, chName, colName, rkName, typeName );
		}
		arg = params.get("dl");
		if ( arg != null )
			sd_s.dl = Integer.parseInt(arg);
		data = getMatchingSuppData( sd_s, cm );
		if (data != null) {
			List<String> result = new ArrayList<String>();
			for ( SuppDatum sd: data )
				result.add(String.format("%d,%1.3f,%1.3f,%d,%d,%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"", 
					sd.sdid, sd.st, sd.et, sd.cid, sd.tid, sd.colid, sd.rid, sd.dl, 
					sd.name, sd.value.replace('\n',Character.MIN_VALUE), sd.chName, sd.typeName, sd.colName, sd.rkName, sd.color ));
			return new TextResult(result);
		}
		return null;
	}
	
	/**
	 * Retrieve the collection of supplementary data types
	 * 
	 * @return List<SuppDatum> the desired supplementary data types (null if an error occurred)
	 */
	public List<SuppDatum> getSuppDataTypes() {

		List<SuppDatum> types = new ArrayList<SuppDatum>();
		try {
			database.useDatabase(dbName);
			sql  = "SELECT * FROM supp_data_type";
			ps = database.getPreparedStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				SuppDatum sd = new SuppDatum( 0.0, 0.0, -1, -1, -1, rs.getInt(1) );
				sd.typeName  = rs.getString(2);
				sd.color     = rs.getString(3);
				sd.dl        = rs.getInt(4);
				types.add(sd);
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.getSuppDataTypes() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
			return null;
		}

		return types;
	}
	
	/**
	 * Get supp data types list in format 'sdtypeid"draw_line"name"color' from database
	 * 
	 * @param drawOnly yield only the drawable types
	 * @return List of Strings with " separated values
	 */
	public RequestResult getSuppTypes(boolean drawOnly) {
		List<String> result = new ArrayList<String>();

		try {
			database.useDatabase(dbName);
			sql = "SELECT * FROM supp_data_type";
			if ( drawOnly )
				sql = sql + " WHERE draw_line=1";
			rs = database.getPreparedStatement(sql + " ORDER BY supp_data_type").executeQuery();
			while (rs.next()) {
				result.add(String.format("%d\"%d\"%s\"%s", rs.getInt(1), rs.getInt(4), rs.getString(2), rs.getString(3)));
			}
			rs.close();

		} catch (Exception e) {
			logger.log(Level.SEVERE, "SQLDataSource.defaultGetSuppdataTypes() failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
		}

		return new TextResult(result);
	}
}
