package gov.usgs.volcanoes.vdx.data;

import cern.colt.matrix.DoubleMatrix2D;
import gov.usgs.math.DownsamplingType;
import gov.usgs.plot.data.GenericDataMatrix;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.util.UtilException;
import gov.usgs.volcanoes.vdx.db.VDXDatabase;
import gov.usgs.volcanoes.vdx.server.RequestResult;
import gov.usgs.volcanoes.vdx.server.TextResult;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;



/**
 * SQL data source. Store reference to VDX database and provide methods to init default database
 * structure.
 * 
 * @author Dan Cervelli, Loren Antolik, Bill Tollett
 */
public abstract class SQLDataSource implements DataSource {

  protected VDXDatabase database;
  protected String vdxName;
  protected String dbName;
  protected static Logger logger = Logger.getLogger("gov.usgs.volcanoes.vdx.data.SQLDataSource");

  protected Statement st;
  protected PreparedStatement ps;
  protected ResultSet rs;
  protected String sql;
  protected String sqlCount;
  private int maxrows = 0;

  /**
   * Initialize the data source. Concrete realization see in the inherited classes
   * 
   * @param params config file
   */
  public abstract void initialize(ConfigFile params);

  /**
   * Get database suffix. Concrete realization see in the inherited classes
   * 
   * @return type string
   */
  public abstract String getType();

  public abstract boolean getChannelsFlag();

  public abstract boolean getTranslationsFlag();

  public abstract boolean getRanksFlag();

  public abstract boolean getColumnsFlag();

  public abstract boolean getMenuColumnsFlag();

  public abstract boolean getChannelTypesFlag();

  /**
   * Check if database exist. Concrete realization see in the inherited classes
   * 
   * @return true if success
   */
  public abstract boolean databaseExists();

  /**
   * Create database. Concrete realization see in the inherited classes
   * 
   * @return true if success
   */
  public abstract boolean createDatabase();

  /**
   * Disconnect from database. Concrete realization see in the inherited classes
   */
  public abstract void disconnect();

  /**
   * Getter for maxrows.
   * 
   * @return maxrows
   */
  public int getMaxRows() {
    return maxrows;
  }

  /**
   * Setter for maxrows.
   * 
   * @param maxrows limit on number of rows to retrieve
   */
  protected void setMaxRows(int maxrows) {
    this.maxrows = maxrows;
  }

  /**
   * Get size of result set.
   * 
   * @param rs ResultSet to query
   * @return Count of records in given ResultSet
   * @throws SQLException if there's a problem with the ResultSet
   */
  public static int getResultSetSize(ResultSet rs) throws SQLException {
    int size = 0;
    int currentRow = rs.getRow();
    if (rs != null) {
      rs.beforeFirst();
      rs.last();
      size = rs.getRow();
      if (currentRow == 0) {
        rs.beforeFirst();
      } else {
        rs.absolute(currentRow);
      }
    }
    return size;
  }

  /**
   * Get error result.
   * 
   * @param errMessage message to pack
   * @return TextResult which contains error message
   */
  public static RequestResult getErrorResult(String errMessage) {
    List<String> text = new ArrayList<String>();
    text.add(errMessage);
    TextResult result = new TextResult(text);
    result.setError(true);
    return result;
  }

  /**
   * Get SQL for downsampling.
   * 
   * @param sql Query to compress
   * @param ds Type of compressing: currently decimate/mean by time interval/none
   * @param dsInt time interval to average values, in seconds
   * @return sql that get only subset of records from original sql
   * @throws UtilException in case of unknown downsampling type
   */
  public static String getDownsamplingSQL(String sql, String timeColumn, DownsamplingType ds,
      int dsInt) throws UtilException {
    
    if (!ds.equals(DownsamplingType.NONE) && dsInt <= 1) {
      throw new UtilException("Downsampling interval should be more than 1");
    }
    
    if (ds.equals(DownsamplingType.NONE)) {
      return sql;
    } else if (ds.equals(DownsamplingType.DECIMATE)) {
      sql = sql.substring(0, sql.toUpperCase().lastIndexOf("ORDER BY") - 1);
      return "SELECT * FROM(SELECT fullQuery.*, @row := @row+1 AS rownum FROM (" + sql
          + ") fullQuery, (SELECT @row:=0) r ORDER BY 1 ASC) ranked WHERE rownum % " + dsInt
          + " = 1";
    } else if (ds.equals(DownsamplingType.MEAN)) {
      String sqlSelectClause = sql.substring(6, sql.toUpperCase().indexOf("FROM") - 1);
      String sqlFromWhereClause = sql.substring(sql.toUpperCase().indexOf("FROM") - 1,
          sql.toUpperCase().lastIndexOf("ORDER BY") - 1);
      String[] columns = sqlSelectClause.split(",");
      String avgSQL = "SELECT ";
      for (String column : columns) {
        String groupFunction = "AVG";
        String[] columnParts = column.trim().split("\\sas\\s");
        if (columnParts[0].equals(timeColumn)) {
          groupFunction = "MIN";
        } else if (columnParts[0].equals("rid") || columnParts[0].endsWith(".rid")) {
          groupFunction = "MIN";
        }
        if (columnParts.length > 1) {
          avgSQL += groupFunction + "(" + columnParts[0] + ") as " + columnParts[1] + ", ";
        } else {
          avgSQL += groupFunction + "(" + columnParts[0] + "), ";
        }
      }
      avgSQL += "(((" + timeColumn + ") - ?) DIV ?) intNum ";
      avgSQL += sqlFromWhereClause;
      avgSQL += " GROUP BY intNum";
      return avgSQL;
    } else {
      throw new UtilException("Unknown downsampling type: " + ds);
    }
  }

  /**
   * Insert data. Concrete realization see in the inherited classes
   * 
   * @return true if success
   */
  // abstract public void insertData(String channelCode, GenericDataMatrix gdm, boolean
  // translations, boolean ranks, int rid);

  /**
   * Initialize Data Source.
   * 
   * @param params config file
   */
  public void defaultInitialize(ConfigFile params) {

    // common database connection parameters
    String driver = params.getString("vdx.driver");
    String url = params.getString("vdx.url");
    String prefix = params.getString("vdx.prefix");
    database = new VDXDatabase(driver, url, prefix);
    vdxName = params.getString("vdx.name");
    
    // dbName is an additional parameter that VDX classes uses, unlike Winston or Earthworm
    dbName = vdxName + "$" + getType();
    maxrows = Util.stringToInt(params.getString("maxrows"), 0);
  }

  /**
   * Close database connection.
   */
  public void defaultDisconnect() {
    database.close();
  }

  /**
   * Check if VDX database has connection to SQL server.
   * 
   * @return true if successful
   */
  public boolean defaultDatabaseExists() {
    return database.useDatabase(dbName);
  }

  /**
   * Create default VDX database.
   * 
   * @param channels if we need to create channels table
   * @param translations if we need to create translations table
   * @param channelTypes if we need to create channel_types table
   * @param ranks if we need to create ranks table
   * @param columns if we need to create columns table
   * @param menuColumns flag to retrieve database columns or plottable columns
   * @return true if success
   */
  public boolean defaultCreateDatabase(boolean channels, boolean translations, boolean channelTypes,
      boolean ranks, boolean columns, boolean menuColumns) {
    try {

      // create the database on the database server and specify to use this database for all
      // subsequent statements
      database.useRootDatabase();
      ps = database.getPreparedStatement("CREATE DATABASE " + database.getDatabasePrefix() 
                                         + "_" + dbName);
      ps.execute();
      database.useDatabase(dbName);

      // creation of a channels table
      if (channels) {

        // these are the basic channel options, we can add on to this below
        sql = "CREATE TABLE channels (cid INT PRIMARY KEY AUTO_INCREMENT, "
            + "code VARCHAR(16) UNIQUE, name VARCHAR(255), "
            + "lon DOUBLE, lat DOUBLE, height DOUBLE, active TINYINT(1) NOT NULL DEFAULT 1";

        // translations. logically you must have a channels table to have translations
        if (translations) {
          sql = sql + ", tid INT DEFAULT 1 NOT NULL";
        }

        // channel types. logically you must have a channels table to have channel types
        if (channelTypes) {
          sql = sql + ", ctid INT DEFAULT 1 NOT NULL";
          ps.execute("CREATE TABLE channel_types (ctid INT PRIMARY KEY AUTO_INCREMENT, "
              + "name VARCHAR(16) UNIQUE, user_default TINYINT(1) DEFAULT 0 NOT NULL)");
          ps.execute("INSERT INTO channel_types (name) VALUES ('DEFAULT')");
        }

        // complete the channels sql statement and execute it
        sql = sql + ")";
        ps.execute(sql);
      }

      if (columns) {
        ps.execute("CREATE TABLE columns (colid INT PRIMARY KEY AUTO_INCREMENT, "
            + "idx INT, name VARCHAR(255) UNIQUE, description VARCHAR(255), unit VARCHAR(255), "
            + "checked TINYINT, active TINYINT, bypassmanipulations TINYINT, accumulate TINYINT)");
      }

      if (menuColumns) {
        ps.execute("CREATE TABLE columns_menu (colid INT PRIMARY KEY AUTO_INCREMENT, "
            + "idx INT, name VARCHAR(255) UNIQUE, description VARCHAR(255), unit VARCHAR(255), "
            + "checked TINYINT, active TINYINT, bypassmanipulations TINYINT, accumulate TINYINT)");
      }

      // the usage of ranks does not depend on there being a channels table
      if (ranks) {
        ps.execute("CREATE TABLE ranks (rid INT PRIMARY KEY AUTO_INCREMENT, "
            + "name VARCHAR(24) UNIQUE, rank INT(10) UNSIGNED DEFAULT 0 NOT NULL, "
            + "user_default TINYINT(1) DEFAULT 0 NOT NULL)");
      }

      ps.execute("CREATE TABLE supp_data (sdid INT NOT NULL AUTO_INCREMENT, st DOUBLE NOT NULL, "
          + "et DOUBLE, sdtypeid INT NOT NULL, sd_short VARCHAR(90) NOT NULL, sd TEXT NOT NULL, "
          + "PRIMARY KEY (sdid))");

      ps.execute("CREATE TABLE supp_data_type (sdtypeid INT NOT NULL AUTO_INCREMENT, "
          + "supp_data_type VARCHAR(20), supp_color VARCHAR(6) NOT NULL, draw_line TINYINT, "
          + "PRIMARY KEY (sdtypeid), UNIQUE KEY (supp_data_type) )");

      sql = "CREATE TABLE supp_data_xref ( sdid INT NOT NULL, cid INT NOT NULL, "
          + "colid INT NOT NULL, ";
      String key = "UNIQUE KEY (sdid,cid,colid";
      if (ranks) {
        sql = sql + "rid INT NOT NULL, ";
        key = key + ",rid";
      }
      ps.execute(sql + key + "))");

      sql = "CREATE TABLE channelmetadata ( cmid INT NOT NULL AUTO_INCREMENT, cid INT NOT NULL, "
          + "colid INT NOT NULL, ";
      if (ranks) {
        sql = sql + "rid INT NOT NULL, ";
      }
      sql = sql + "name VARCHAR(20) NOT NULL, value TEXT NOT NULL, UNIQUE KEY (cmid,cid,colid";
      if (ranks) {
        sql = sql + ",rid";
      }
      ps.execute(sql + "))");

      logger.log(Level.INFO, "SQLDataSource.defaultCreateDatabase(" + database.getDatabasePrefix()
          + "_" + dbName + ") succeeded. ");
      return true;

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultCreateDatabase(" + database.getDatabasePrefix()
          + "_" + dbName + ") failed.", e);
    }

    return false;
  }

  /**
   * Get channel name.
   * 
   * @param plural if we need channel name in the plural form
   * @return channel name
   */
  public String getChannelName(boolean plural) {
    return plural ? "Channels" : "Channel";
  }

  /**
   * Create default channel from values in the columns table.
   * 
   * @param channel channel object
   * @param tid translation id
   * @param channels if we need to add record in 'channels' table
   * @param translations if we need to add tid field in channel table
   * @param ranks if we need to add rid field in channel table
   * @param columns if we need to create a channel table based on the columns table
   * @return true if success
   */
  public boolean defaultCreateChannel(Channel channel, int tid, boolean channels,
      boolean translations, boolean ranks, boolean columns) {
    return defaultCreateChannel(channel.getCode(), channel.getName(), channel.getLon(),
        channel.getLat(), channel.getHeight(), channel.getActive(), tid, channels, translations,
        ranks, columns);
  }

  /**
   * Create default channel from values in the columns table.
   * 
   * @param channelCode channel code
   * @param channelName channel name
   * @param lon longitude
   * @param lat latitude
   * @param height height
   * @param active active status
   * @param tid translation id
   * @param channels if we need to add record in 'channels' table
   * @param translations if we need to add tid field in channel table
   * @param ranks if we need to add rid field in channel table
   * @param columns if we need to create a channel table based on the columns table
   * @return true if success
   */
  public boolean defaultCreateChannel(String channelCode, String channelName, double lon,
      double lat, double height, int active, int tid, boolean channels, boolean translations,
      boolean ranks, boolean columns) {

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
        String columnList = "code, name, lon, lat, height, active";
        String variableList = "?,?,?,?,?,?";

        if (translations) {
          columnList = columnList + ",tid";
          variableList = variableList + ",?";
        }

        ps = database.getPreparedStatement("INSERT INTO channels (" + columnList + ") VALUES (" 
           + variableList + ")");

        ps.setString(1, channelCode);
        ps.setString(2, channelName);
        if (Double.isNaN(lon)) {
          ps.setNull(3, java.sql.Types.DOUBLE);
        } else {
          ps.setDouble(3, lon);
        }
        if (Double.isNaN(lat)) {
          ps.setNull(4, java.sql.Types.DOUBLE);
        } else {
          ps.setDouble(4, lat);
        }
        if (Double.isNaN(height)) {
          ps.setNull(5, java.sql.Types.DOUBLE);
        } else {
          ps.setDouble(5, height);
        }
        ps.setInt(6, active);
        if (translations) {
          ps.setInt(7, tid);
        }
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

            // when using ranks, the primary key is the combo of j2ksec and rid, otherwise, it's
            // just the j2ksec
          } else {
            sql = sql + ",PRIMARY KEY(j2ksec)";
          }

          // place the closing parenthesis and execute the sql statement
          sql = sql + ",KEY index_j2ksec (j2ksec))";
          ps = database.getPreparedStatement(sql);
          ps.execute(sql);
        }
      }

      logger.log(Level.INFO, "SQLDataSource.defaultCreateChannel(" + channelCode + "," + lon + ","
          + lat + ") succeeded. (" + database.getDatabasePrefix() + "_" + dbName + ")");
      return true;

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultCreateChannel(" + channelCode + "," + lon + ","
          + lat + ") failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return false;
  }

  /**
   * Create entry in the channels table and creates a table for that channel.
   * 
   * @param channel Channel
   * @param tid translation id
   * @param azimuth azimuth of the deformation source
   * @param channels if we need to add record in 'channels' table
   * @param translations if we need to add tid field in channel table
   * @param ranks if we need to add rid field in channel table
   * @param columns if we need to create a channel table based on the columns table
   * @return true if successful
   */
  public boolean defaultCreateTiltChannel(Channel channel, int tid, double azimuth,
      boolean channels, boolean translations, boolean ranks, boolean columns) {
    try {
      defaultCreateChannel(channel, tid, channels, translations, ranks, columns);

      // update the channels table with the azimuth value
      String azimuthColumnName = null;
      if (dbName.toLowerCase().contains("tensorstrain")) {
        azimuthColumnName = "natural_azimuth";
      } else {
        azimuthColumnName = "azimuth";
      }
      
      database.useDatabase(dbName);
      
      Channel ch = defaultGetChannel(channel.getCode(), false);
      ps = database.getPreparedStatement("UPDATE channels SET " + azimuthColumnName 
                                       + "  = ? WHERE cid = ?");
      ps.setDouble(1, azimuth);
      ps.setInt(2, ch.getCId());
      ps.execute();
      return true;
    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultCreateTiltChannel() failed.", e);
    }
    return false;
  }

  /**
   * Updates the channels table with the specified translation id.
   * 
   * @param channelCode channel to be updated
   * @param tid new translation id for this channel
   * @return true if success
   */
  public boolean defaultUpdateChannelTranslationId(String channelCode, int tid) {
    try {
      database.useDatabase(dbName);
      ps = database.getPreparedStatement("UPDATE channels SET tid = ? WHERE code = ?");
      ps.setInt(1, tid);
      ps.setString(2, channelCode);
      ps.execute();

      logger.log(Level.INFO, "SQLDataSource.defaultUpdateChannelTranslationID(" + channelCode + ","
          + tid + ") succeeded. (" + database.getDatabasePrefix() + "_" + dbName + ")");
      return true;

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultUpdateChannelTranslationID(" + channelCode
          + "," + tid + ") failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return false;
  }

  /**
   * Create default translation table from values in the columns table.
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

        logger.log(Level.INFO, "SQLDataSource.defaultCreateTranslation() succeeded. ("
            + database.getDatabasePrefix() + "_" + dbName + ")");
        return true;
      }

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultCreateTranslation() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return false;
  }

  /**
   * Insert column.
   * 
   * @param column Column
   * @return true if successful
   */
  public boolean defaultInsertColumn(Column column) {
    try {
      database.useDatabase(dbName);
      ps = database.getPreparedStatement("INSERT IGNORE INTO columns (idx, name, description, "
                                       + "unit, checked, active, bypassmanipulations, accumulate) "
                                       + "VALUES (?,?,?,?,?,?,?,?)");
      ps.setInt(1, column.idx);
      ps.setString(2, column.name);
      ps.setString(3, column.description);
      ps.setString(4, column.unit);
      ps.setBoolean(5, column.checked);
      ps.setBoolean(6, column.active);
      ps.setBoolean(7, column.bypassmanip);
      ps.setBoolean(8, column.accumulate);
      ps.execute();

      logger.log(Level.INFO, "SQLDataSource.defaultInsertColumn(" + column.name + ") succeeded. ("
          + database.getDatabasePrefix() + "_" + dbName + ")");
      return true;

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultInsertColumn(" + column.name + ") failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }
    return false;
  }

  /**
   * Insert plot column.
   * 
   * @param column Column return true if successful
   */
  public boolean defaultInsertMenuColumn(Column column) {
    try {
      database.useDatabase(dbName);
      ps = database.getPreparedStatement("INSERT IGNORE INTO columns_menu (idx, name, description, "
                                       + "unit, checked, active, bypassmanipulations, accumulate) "
                                       + "VALUES (?,?,?,?,?,?,?,?)");
      ps.setInt(1, column.idx);
      ps.setString(2, column.name);
      ps.setString(3, column.description);
      ps.setString(4, column.unit);
      ps.setBoolean(5, column.checked);
      ps.setBoolean(6, column.active);
      ps.setBoolean(7, column.bypassmanip);
      ps.setBoolean(8, column.accumulate);
      ps.execute();

      logger.log(Level.INFO, "SQLDataSource.defaultInsertPlotColumn(" + column.name
          + ") succeeded. (" + database.getDatabasePrefix() + "_" + dbName + ")");
      return true;

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultInsertPlotColumn(" + column.name
          + ") failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }
    return false;
  }

  /**
   * Create new channel type.
   * 
   * @param name channel type display name
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

      logger.log(Level.INFO, "SQLDataSource.defaultInsertChannelType(" + name + ") succeeded. ("
          + database.getDatabasePrefix() + "_" + dbName + ")");

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultInsertChannelType(" + name + ") failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return result;
  }

  /**
   * Inserts rank into database.
   * 
   * @param rank rank to be inserted
   * @return newly inserted rank. null
   */
  public Rank defaultInsertRank(Rank rank) {
    return defaultInsertRank(rank.getName(), rank.getRank(), rank.getUserDefault());
  }

  /**
   * Create new rank.
   * 
   * @param name rank display name
   * @param rank integer value of rank
   * @param userDefault flag to set new rank as default
   * @return Rank object using the specified
   */
  public Rank defaultInsertRank(String name, int rank, int userDefault) {
    Rank result = null;

    try {

      int rid = defaultGetRankId(rank);
      if (rid > 0) {
        return defaultGetRank(rid);
      }

      database.useDatabase(dbName);

      // if updating the default value then set all other default values
      // to 0 (there can only be one row set to default)
      if (userDefault == 1) {
        ps = database.getPreparedStatement("UPDATE ranks set user_default = 0");
        ps.execute();
      }

      // create the new rank
      ps = database.getPreparedStatement("INSERT INTO ranks (name, rank, user_default) "
                                       + "VALUES (?,?,?)");
      ps.setString(1, name);
      ps.setInt(2, rank);
      ps.setInt(3, userDefault);
      ps.execute();

      // get the id of the newly inserted rank
      rs = database.getPreparedStatement("SELECT LAST_INSERT_ID()").executeQuery();
      if (rs.next()) {
        result = defaultGetRank(rs.getInt(1));
      }
      rs.close();

      logger.log(Level.INFO, "SQLDataSource.defaultInsertRank(" + name + "," + rank + ","
          + userDefault + ") succeeded. (" + database.getDatabasePrefix() + "_" + dbName + ")");

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultInsertRank(" + name + "," + rank + ","
          + userDefault + ") failed. (" + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return result;
  }

  /**
   * Inserts a translation in the translations table.
   * 
   * @param channelCode channel code
   * @param gdm generic data matrix containing the translations
   * @return tid translation id of new translation. -1 if failure
   */
  public int defaultInsertTranslation(String channelCode, GenericDataMatrix gdm) {

    // default local variables
    int tid = 1;
    String columns = "";
    String values = "";

    try {
      database.useDatabase(dbName);

      DoubleMatrix2D dm = gdm.getData();
      String[] columnNames = gdm.getColumnNames();

      // iterate through the generic data matrix to get a list of the values
      for (int i = 0; i < columnNames.length; i++) {
        columns += columnNames[i] + ",";
        values += dm.get(0, i) + ",";
      }
      columns += "name";
      values += "'" + channelCode + "'";

      // insert the translation into the database
      ps = database.getPreparedStatement("INSERT INTO translations (" + columns + ") "
                                       + "VALUES (" + values + ")");
      ps.execute();
      tid = defaultGetTranslation(channelCode, gdm);

      logger.log(Level.INFO, "SQLDataSource.defaultInsertTranslation() succeeded. ("
          + database.getDatabasePrefix() + "_" + dbName + ")");

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultInsertTranslation() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return tid;
  }

  /**
   * Get channel from database.
   * 
   * @param cid channel id
   * @param channelTypes if the channels table has channel types
   * @return channel
   */
  public Channel defaultGetChannel(int cid, boolean channelTypes) {
    Channel ch = null;
    String code;
    String name;
    int active;
    double lon;
    double lat;
    double height;
    int ctid = 0;

    try {
      database.useDatabase(dbName);

      sql = "SELECT code, name, lon, lat, height, active ";
      if (channelTypes) {
        sql = sql + ",ctid ";
      }
      sql = sql + "FROM  channels ";
      sql = sql + "WHERE cid = ?";

      ps = database.getPreparedStatement(sql);
      ps.setInt(1, cid);
      rs = ps.executeQuery();
      if (rs.next()) {
        code = rs.getString(1);
        name = rs.getString(2);
        lon = rs.getDouble(3);
        if (rs.wasNull()) {
          lon = Double.NaN;
        }
        lat = rs.getDouble(4);
        if (rs.wasNull()) {
          lat = Double.NaN;
        }
        height = rs.getDouble(5);
        if (rs.wasNull()) {
          height = Double.NaN;
        }
        active = rs.getInt(6);
        if (channelTypes) {
          ctid = rs.getInt(7);
        }

        ch = new Channel(cid, code, name, lon, lat, height, active, ctid);
      }
      rs.close();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannel(cid) failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return ch;
  }

  /**
   * Get channel from database.
   * 
   * @param code channel code
   * @param channelTypes if the channels table has channel types
   * @return channel
   */
  public Channel defaultGetChannel(String code, boolean channelTypes) {
    Channel ch = null;

    try {
      database.useDatabase(dbName);
      ps = database.getPreparedStatement("SELECT cid FROM channels WHERE code = ? ");
      ps.setString(1, code);
      rs = ps.executeQuery();
      if (rs.next()) {
        int cid = rs.getInt(1);
        ch = defaultGetChannel(cid, channelTypes);
      }
      rs.close();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannel(code) failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return ch;
  }

  /**
   * Get channels list from database.
   * 
   * @param channelTypes if the channels table has channel types
   * @return List of Channels
   */
  public List<Channel> defaultGetChannelsList(boolean channelTypes) {
    List<Channel> result = new ArrayList<Channel>();
    Channel ch;
    int active;
    int cid;
    int ctid;
    String code;
    String name;
    double lon;
    double lat;
    double height;
    double azimuth;

    try {
      database.useDatabase(dbName);

      sql = "SELECT cid, code, name, lon, lat, height, active ";
      if (dbName.toLowerCase().contains("tensorstrain")) {
        sql = sql + ", natural_azimuth ";
      } else if (dbName.toLowerCase().contains("tilt")) {
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

        cid = rs.getInt(1);
        code = rs.getString(2);
        name = rs.getString(3);
        lon = rs.getDouble(4);
        if (rs.wasNull()) {
          lon = Double.NaN;
        }
        lat = rs.getDouble(5);
        if (rs.wasNull()) {
          lat = Double.NaN;
        }
        height = rs.getDouble(6);
        if (rs.wasNull()) {
          height = Double.NaN;
        }
        active = rs.getInt(7);
        azimuth = rs.getDouble(8);
        if (rs.wasNull()) {
          azimuth = Double.NaN;
        }
        if (channelTypes) {
          ctid = rs.getInt(9);
        } else {
          ctid = 0;
        }
        ch = new Channel(cid, code, name, lon, lat, height, active, azimuth, ctid);
        result.add(ch);
      }
      rs.close();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannelsList() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return result;
  }

  /**
   * Get channels list from database.
   * 
   * @param channelTypes if the channels table has channel types
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
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannels() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return result;
  }

  /**
   * Get channel types list in format "ctid:name" from database.
   * 
   * @return List of Strings with : separated values
   */
  public List<String> defaultGetChannelTypes() {
    List<String> result = new ArrayList<String>();

    try {
      database.useDatabase(dbName);
      rs = database
          .getPreparedStatement("SELECT ctid, name, user_default FROM channel_types ORDER BY name")
          .executeQuery();
      while (rs.next()) {
        result.add(String.format("%d:%s:%d", rs.getInt(1), rs.getString(2), rs.getInt(3)));
      }
      rs.close();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannelTypes() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return result;
  }

  /**
   * Get rank from database.
   * 
   * @param rank user defined rank
   * @return rank, null if not found
   */
  public Rank defaultGetRank(Rank rank) {
    return defaultGetRank(defaultGetRankId(rank.getRank()));
  }

  /**
   * Get rank from database.
   * 
   * @param rid rank id
   * @return rank
   */
  public Rank defaultGetRank(int rid) {
    Rank result = null;

    try {
      database.useDatabase(dbName);
      ps = database.getPreparedStatement("SELECT rid, name, rank, user_default FROM ranks "
                                       + "WHERE rid = ?");
      ps.setInt(1, rid);
      rs = ps.executeQuery();
      if (rs.next()) {
        result = new Rank(rs.getInt(1), rs.getString(2), rs.getInt(3), rs.getInt(4));
      }
      rs.close();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetRank() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return result;
  }

  /**
   * Get rank id from database.
   * 
   * @param rank rank ID
   * @return rank id
   */
  public int defaultGetRankId(int rank) {
    int result = -1;

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
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetRankID() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return result;
  }

  /**
   * Get ranks list in format "rid:name:rank:user_default" from database.
   * 
   * @return List of Strings with : separated values
   */
  public List<String> defaultGetRanks() {
    List<String> result = new ArrayList<String>();

    try {
      database.useDatabase(dbName);
      rs = database
          .getPreparedStatement("SELECT rid, name, rank, user_default FROM ranks ORDER BY rank")
          .executeQuery();
      while (rs.next()) {
        result.add(String.format("%d:%s:%d:%d", rs.getInt(1), rs.getString(2), rs.getInt(3),
            rs.getInt(4)));
      }
      rs.close();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetRanks() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return result;
  }

  /**
   * Get number of ranks from database.
   * 
   * @return number of ranks, default 1
   */
  public int defaultGetNumberOfRanks() {
    int result = 1;

    try {
      database.useDatabase(dbName);
      rs = database.getPreparedStatement("SELECT COUNT(*) FROM ranks").executeQuery();
      if (rs.next()) {
        result = rs.getInt(1);
      }
      rs.close();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetNumberOfRanks() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return result;
  }

  /**
   * Gets translation id from database using the parameters passed. Used to determine if the
   * translation exists in the database for potentially inserting a new translation.
   * 
   * @param channelCode channel code
   * @param gdm generic data matrix containing the translations
   * @return tid translation id of the translation. -1 if not found.
   */
  public int defaultGetTranslation(String channelCode, GenericDataMatrix gdm) {
    int result = 1;

    try {
      database.useDatabase(dbName);

      // iterate through the generic data matrix to get a list of the columns and their values
      DoubleMatrix2D dm = gdm.getData();
      String[] columnNames = gdm.getColumnNames();
      sql = "";
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
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetTranslation() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return result;
  }

  /**
   * lookup translation id based on channel code.
   * 
   * @param channelCode channel code to lookup
   * @return translation id, 1 if not found
   */
  public int defaultGetChannelTranslationId(String channelCode) {
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
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannelTranslationID() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return result;
  }

  /**
   * Get List of columns from the database param menuColumns flag to retrieve database columns or
   * plottable columns.
   * 
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
   * Getter for columns.
   * 
   * @param allColumns flag to retrieve only active columns from table
   * @param menuColumns flag to retrieve database columns or plottable columns
   * @return List of Columns, ordered by index
   */
  public List<Column> defaultGetColumns(boolean allColumns, boolean menuColumns) {

    Column column;
    List<Column> columns = new ArrayList<Column>();
    boolean checked;
    boolean active;
    boolean bypassmanipulations;
    boolean accumulate;
    String tableName = "";

    if (menuColumns) {
      tableName = "columns_menu";
    } else {
      tableName = "columns";
    }

    try {
      database.useDatabase(dbName);
      sql = "SELECT idx, name, description, unit, checked, active, bypassmanipulations, "
          + "accumulate ";
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
        if (rs.getInt(8) == 0) {
          accumulate = false;
        } else {
          accumulate = true;
        }
        column = new Column(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getString(4),
            checked, active, bypassmanipulations, accumulate);
        columns.add(column);
      }
      rs.close();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetColumns() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return columns;
  }

  /**
   * Get channel from database.
   * 
   * @param colid column id
   * @return column, null if not found
   */
  public Column defaultGetColumn(int colid) {
    Column col = null;
    int idx;
    String name;
    String description;
    String unit;
    boolean checked;
    boolean active;
    boolean bypassmanipulations;
    boolean accumulate;

    try {
      database.useDatabase(dbName);

      sql = "SELECT idx, name, description, unit, checked, active, bypassmanipulations, "
          + "accumulate ";
      sql += "FROM  columns ";
      sql += "WHERE colid = ?";

      ps = database.getPreparedStatement(sql);
      ps.setInt(1, colid);
      rs = ps.executeQuery();
      if (rs.next()) {
        idx = rs.getInt(1);
        name = rs.getString(2);
        description = rs.getString(3);
        unit = rs.getString(4);
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
        if (rs.getInt(8) == 0) {
          accumulate = false;
        } else {
          accumulate = true;
        }
        col = new Column(idx, name, description, unit, checked, active, bypassmanipulations,
            accumulate);
      }
      rs.close();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetColumn(colid) failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return col;
  }

  /**
   * Get channel from database.
   * 
   * @param name column name
   * @return column
   */
  public Column defaultGetColumn(String name) {
    Column col = null;

    try {
      database.useDatabase(dbName);
      ps = database.getPreparedStatement("SELECT colid FROM columns WHERE name = ? ");
      ps.setString(1, name);
      rs = ps.executeQuery();
      if (rs.next()) {
        int colid = rs.getInt(1);
        col = defaultGetColumn(colid);
      }
      rs.close();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetChannel(name) failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return col;
  }

  /**
   * Get options list in format "idx:code:name" from database.
   * 
   * @param type suffix of table name
   * @return List of Strings with : separated values
   */
  public List<String> defaultGetOptions(String type) {
    List<String> result = new ArrayList<String>();

    try {
      database.useDatabase(dbName);
      sql = "SELECT idx, code, name ";
      sql += "FROM   options_" + type + " ";
      sql += "ORDER BY idx";
      rs = database.getPreparedStatement(sql).executeQuery();
      while (rs.next()) {
        result.add(String.format("%d:%s:%s", rs.getInt(1), rs.getString(2), rs.getString(3)));
      }
      rs.close();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetOptions() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return result;
  }

  /**
   * Gets the most recent timestamp in the database for the specified channel.
   * 
   * @param channelCode channel to check
   * @return most recent timestamp
   */
  public synchronized Date defaultGetLastDataTime(String channelCode, String nullField,
      boolean pollhist) {

    Date lastDataTime;

    if (pollhist) {
      lastDataTime = new Date(0);
      try {
        database.useDatabase(dbName);
        sql = "SELECT max(j2ksec) FROM " + channelCode;
        if (nullField.length() > 0) {
          sql += " WHERE " + nullField + " IS NOT NULL";
        }
        ps = database.getPreparedStatement(sql);
        rs = ps.executeQuery();
        rs.next();
        double result = rs.getDouble(1);
        if (!rs.wasNull()) {
          lastDataTime = Util.j2KToDate(result);
        }
        rs.close();

      } catch (Exception e) {
        logger.log(Level.SEVERE, "SQLDataSource.defaultGetLastDataTime() failed. ("
            + database.getDatabasePrefix() + "_" + dbName + ")", e);
      }

    } else {
      lastDataTime = new Date();
    }

    return lastDataTime;
  }

  /**
   * Get data from database.
   * 
   * @param cid channel id
   * @param rid rank id
   * @param st start time
   * @param et end time
   * @param translations if the database has translations
   * @param ranks if the database has ranks
   * @param maxrows limit on number of rows returned
   * @param ds Downsampling type
   * @param dsInt argument for downsampling
   * @return GenericDataMatrix containing the data
   * @throws UtilException if downsampling fails or returns too many rows
   */
  public GenericDataMatrix defaultGetData(int cid, int rid, double st, double et,
      boolean translations, boolean ranks, int maxrows, DownsamplingType ds, int dsInt)
      throws UtilException {

    double[] dataRow;
    List<double[]> pts = new ArrayList<double[]>();
    GenericDataMatrix result = null;
    List<Column> columns = new ArrayList<Column>();
    Column column;
    int columnsReturned = 0;

    try {
      database.useDatabase(dbName);

      // look up the channel code from the channels table, which is the name of the table to query
      // channel types is false because at this point we don't care about that, just trying to get
      // the channel name
      final Channel channel = defaultGetChannel(cid, false);
      columns = defaultGetColumns(false, false);

      // calculate the num of rows to limit the query to
      int tempmaxrows;
      if (rid != 0) {
        tempmaxrows = maxrows;
      } else {
        tempmaxrows = maxrows * defaultGetNumberOfRanks();
      }

      // if we are getting ranked data back, then we need to include the rid, otherwise, just add in
      // a field for j2ksec
      if (ranks) {
        columnsReturned = columns.size() + 2;
      } else {
        columnsReturned = columns.size() + 1;
      }

      // SELECT sql
      sql = "SELECT j2ksec";

      if (ranks) {
        sql += ", c.rid";
      }

      for (int i = 0; i < columns.size(); i++) {
        column = columns.get(i);
        if (translations) {
          sql += ",a." + column.name + " * b.c" + column.name + " + b.d" + column.name + " as "
              + column.name + " ";
        } else {
          sql += ",a." + column.name + " ";
        }
      }

      // FROM sql
      sql += "FROM " + channel.getCode() + " a ";
      if (translations) {
        sql += "INNER JOIN translations b on a.tid = b.tid ";
      }
      if (ranks) {
        sql += "INNER JOIN ranks        c on a.rid = c.rid ";
      }

      // WHERE sql
      sql += "WHERE j2ksec >= ? ";
      sql += "AND   j2ksec <= ? ";

      sqlCount = "SELECT COUNT(*) FROM (SELECT 1 FROM " + channel.getCode()
               + " a INNER JOIN ranks c ON a.rid=c.rid ";
      sqlCount += "WHERE j2ksec >= ? AND j2ksec <= ? ";

      // BEST AVAILABLE DATA query
      if (ranks && rid != 0) {
        sql += "AND   c.rid  = ? ";
        sqlCount += "AND c.rid = ? ";
      }

      sql += "ORDER BY a.j2ksec ASC";
      sqlCount += "ORDER BY a.j2ksec ASC";

      if (ranks && rid == 0) {
        sql += ", c.rank DESC";
        sqlCount += ", c.rank DESC";
      }

      if (ranks && rid != 0) {
        try {
          sql = getDownsamplingSQL(sql, "j2ksec", ds, dsInt);
        } catch (UtilException e) {
          throw new UtilException("Can't downsample dataset: " + e.getMessage());
        }
      }

      if (maxrows != 0) {
        sql += " LIMIT " + (tempmaxrows + 1);

        // If the dataset has a maxrows paramater, check that the number of requested rows doesn't
        // exceed that number prior to running the full query. This can save a decent amount of time
        // for large queries. Note that this only applies for non-downsampled queries. This is done
        // for two reasons: 1) If the user is downsampling, they already know they're dealing with 
        // a lot of data and 2) the way MySQL handles the multiple nested queries that would result 
        // makes it slower than just doing the full query to begin with.
        if (ds.equals(DownsamplingType.NONE)) {
          ps = database.getPreparedStatement(sqlCount + " LIMIT " + (tempmaxrows + 1) + ") as T");
          ps.setDouble(1, st);
          ps.setDouble(2, et);
          if (ranks && rid != 0) {
            ps.setInt(3, rid);
          }
          rs = ps.executeQuery();
          if (rs.next() && rs.getInt(1) > tempmaxrows) {
            throw new UtilException("Max rows (" + maxrows + " rows) for source '" + dbName
                + "' exceeded. Please use downsampling.");
          }
        }
      }

      ps = database.getPreparedStatement(sql);
      if (ds.equals(DownsamplingType.MEAN)) {
        ps.setDouble(1, st);
        ps.setInt(2, dsInt);
        ps.setDouble(3, st);
        ps.setDouble(4, et);
        if (ranks && rid != 0) {
          ps.setInt(5, rid);
        }
      } else {
        ps.setDouble(1, st);
        ps.setDouble(2, et);
        if (ranks && rid != 0) {
          ps.setInt(3, rid);
        }
      }
      rs = ps.executeQuery();

      // Check for the amount of data returned in a downsampled query. Non-downsampled queries are
      // checked above.
      if (!ds.equals(DownsamplingType.NONE) && maxrows != 0 && getResultSetSize(rs) > tempmaxrows) {
        throw new UtilException("Max rows (" + maxrows + " rows) for source '" + dbName
            + "' exceeded. Please downsample further.");
      }

      double tempJ2ksec = Double.MAX_VALUE;

      // loop through each result and add to the list
      while (rs.next()) {

        // if this is a new j2ksec, then save this data, as it contains the highest rank
        if (Double.compare(tempJ2ksec, rs.getDouble(1)) != 0) {

          // loop through each of the columns and convert to Double.NaN if it was null in the DB
          dataRow = new double[columnsReturned];
          for (int i = 0; i < columnsReturned; i++) {
            dataRow[i] = getDoubleNullCheck(rs, i + 1);
          }
          pts.add(dataRow);
        }
        tempJ2ksec = rs.getDouble(1);
      }
      rs.close();

      // if no data rows were returned, instantiate a data matrix with a single row with all null
      // values
      if (pts.size() == 0) {
        dataRow = new double[columnsReturned];
        for (int i = 0; i < columnsReturned; i++) {
          dataRow[i] = Double.NaN;
        }
        pts.add(dataRow);
      }

      result = new GenericDataMatrix(pts);

    } catch (SQLException e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetData() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return result;
  }

  /**
   * Retrieves the value of the designated column in the current row of <code>ResultSet</code>
   * object as a <code>double</code> in the Java programming language
   * 
   * @param rs result set to extract data
   * @param columnIndex the first column is 1, the second is 2, ...
   * @return column value; not like ResultSet.getData(), if the value is SQL <code>NULL</code>, the
   *         value returned is <code>Double.NaN</code>
   * @throws SQLException if there's a problem with the ResultSet
   */
  public double getDoubleNullCheck(ResultSet rs, int columnIndex) throws SQLException {
    double value = rs.getDouble(columnIndex);
    if (rs.wasNull()) {
      value = Double.NaN;
    }
    return value;
  }

  /**
   * Retrieves the value of the designated column in the current row of <code>ResultSet</code>
   * object as a <code>int</code> in the Java programming language
   * 
   * @param rs result set to extract data
   * @param columnIndex the first column is 1, the second is 2, ...
   * @return column value; not like ResultSet.getData(), if the value is SQL <code>NULL</code>, the
   *         value returned is <code>Integer.MIN_VALUE</code>
   * @throws SQLException if there's a problem with the ResultSet
   */
  public int getIntNullCheck(ResultSet rs, int columnIndex) throws SQLException {
    int value = rs.getInt(columnIndex);
    if (rs.wasNull()) {
      value = Integer.MIN_VALUE;
    }
    return value;
  }

  /**
   * Insert data.
   * 
   * @param channelCode table name
   * @param gdm 2d matrix of data
   * @param translations if the database uses translations
   * @param ranks if the database uses ranks
   * @param rid rank id
   */
  public void defaultInsertData(String channelCode, GenericDataMatrix gdm, boolean translations,
      boolean ranks, int rid) {

    double value;
    int tid = 1;
    String[] columnNames = gdm.getColumnNames();
    DoubleMatrix2D data = gdm.getData();
    StringBuffer columnBuffer = new StringBuffer();
    StringBuffer valuesBuffer = new StringBuffer();
    StringBuffer dupsBuffer = new StringBuffer();
    String output;
    String base;

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

      // build the ON UPDATE clause
      for (int i = 0; i < columnNames.length; i++) {
        if (!columnNames[i].equals("j2ksec")) {
          dupsBuffer.append(columnNames[i] + "=VALUES(" + columnNames[i] + "),");
        }
      }

      // add in translation related information
      if (translations) {
        tid = defaultGetChannelTranslationId(channelCode);
        columnBuffer.append(",tid");
        valuesBuffer.append("," + tid);
      }

      // add in rank related information
      if (ranks) {
        columnBuffer.append(",rid");
        valuesBuffer.append("," + rid);
      }

      sql = "INSERT INTO " + channelCode + " (" + columnBuffer.toString() + ") VALUES ("
          + valuesBuffer.toString() + ") ";
      sql += "ON DUPLICATE KEY UPDATE "
          + dupsBuffer.toString().substring(0, dupsBuffer.toString().length() - 1);
      base = channelCode + "(";

      ps = database.getPreparedStatement(sql);

      // loop through each of the rows and insert data
      for (int i = 0; i < gdm.rows(); i++) {
        output = base;

        // loop through each of the columns and set it
        for (int j = 0; j < columnNames.length; j++) {

          // check for null values and use the correct setter function if so
          value = data.getQuick(i, j);
          if (Double.isNaN(value)) {
            ps.setNull(j + 1, java.sql.Types.DOUBLE);
          } else {
            ps.setDouble(j + 1, value);
          }
          output = output + value + ",";
        }
        ps.execute();
        if (translations) {
          output += tid + ",";
        }
        
        if (ranks) {
          output += rid + ",";
        }
      }

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultInsertData() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }
  }

  /**
   * Insert a piece of metadata.
   * 
   * @param md the MetaDatum to be added
   */
  public void insertMetaDatum(MetaDatum md) {
    try {
      database.useDatabase(dbName);

      sql = "INSERT INTO channelmetadata (cid,colid,rid,name,value) VALUES (" + md.cid + ","
          + md.colid + "," + md.rid + ",\"" + md.name + "\",\"" + md.value + "\");";

      ps = database.getPreparedStatement(sql);

      ps.execute();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.insertMetaDatum() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }
  }

  /**
   * Update a piece of metadata.
   * 
   * @param md the MetaDatum to be updated
   */
  public void updateMetaDatum(MetaDatum md) {
    try {
      database.useDatabase(dbName);

      sql = "UPDATE channelmetadata SET cid='" + md.cid + "', colid='" + md.colid + "', rid='"
          + md.rid + "', name='" + md.name + "', value='" + md.value + "' WHERE cmid='" + md.cmid
          + "'";

      ps = database.getPreparedStatement(sql);

      ps.execute();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.updateMetaDatum() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }
  }

  /**
   * Retrieve a piece of metadata.
   * 
   * @param cmid the ID of the metadata to retrieve
   * @return MetaDatum the desired metadata (null if not found)
   */
  public MetaDatum getMetaDatum(int cmid) {
    try {
      database.useDatabase(dbName);
      MetaDatum md = null;
      sql = "SELECT * FROM channelmetadata WHERE cmid = " + cmid;
      ps = database.getPreparedStatement(sql);
      rs = ps.executeQuery();
      if (rs.next()) {
        md = new MetaDatum();
        md.cmid = rs.getInt(1);
        md.cid = rs.getInt(2);
        md.colid = rs.getInt(3);
        md.rid = rs.getInt(4);
        md.name = rs.getString(5);
        md.value = rs.getString(6);
        md.chName = rs.getString(7);
        md.colName = rs.getString(8);
        md.rkName = rs.getString(9);
      }
      rs.close();
      return md;
    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.getMetaDatum() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
      return null;
    }
  }

  /**
   * Retrieve a collection of metadata.
   * 
   * @param md the pattern to match (integers < 0 & null strings are ignored)
   * @param cm = "is the name of the columns table coulmns_menu?"
   * @return the desired metadata (null if an error occurred)
   */
  public List<MetaDatum> getMatchingMetaData(MetaDatum md, boolean cm, String source) {
    try {
      database.useDatabase(dbName);
      sql = "SELECT MD.*, CH.code, ";

      // TODO: fix. shouldn't this be taken care of by overriding the method in subclasses? -tjp
      if (source.contains("rsam")) {
        // RSAM has no rank
        sql += "COL.name FROM channelmetadata MD INNER JOIN channels CH ON MD.cid=CH.cid "
             + "INNER JOIN columns" + (cm ? "_menu" : "") + " COL ON MD.colid=COL.colid WHERE ";
      } else if (source.contains("hypocenters") || source.contains("lightning")) {
        sql = "SELECT MD.*, RK.name FROM channelmetadata MD "
            + "INNER JOIN ranks RK ON MD.rid=RK.rid WHERE ";
      } else {
        sql += "COL.name, RK.name FROM channelmetadata MD INNER JOIN channels CH ON MD.cid=CH.cid "
             + "INNER JOIN columns" + (cm ? "_menu" : "") + " COL ON MD.colid=COL.colid "
             + "INNER JOIN ranks RK ON " + "MD.rid=RK.rid WHERE ";
      }

      List<String> wheres = new ArrayList<String>();

      if (md.chName != null) {
        if (md.cid < 0) {
          wheres.add("CH.code='" + md.chName + "'");
        } else {
          wheres.add("CH.cid IN (" + md.chName + ")");
        }
      } else if (md.cid >= 0) {
        wheres.add("MD.cid=" + md.cid);
      }
      
      if (md.colName != null) {
        if (md.colid < 0) {
          wheres.add("COL.name='" + md.colName + "'");
        } else {
          wheres.add("MD.colid IN (" + md.colName + ")");
        }
      } else if (md.colid >= 0) {
        wheres.add("MD.colid=" + md.colid);
      }
      
      if (md.rkName != null) {
        wheres.add("RK.name='" + md.rkName + "'");
      } else if (md.rid >= 0) {
        wheres.add("MD.rid=" + md.rid);
      }
      
      if (md.name != null) {
        wheres.add("MD.name=" + md.name);
      }
      
      if (md.value != null) {
        wheres.add("MD.value=" + md.value);
      }
      
      StringBuffer where = new StringBuffer();
      boolean first = true;
      for (String s : wheres) {
        if (first) {
          where.append(s);
          first = false;
        } else {
          where.append(" AND " + s);
        }
      }

      logger.info("SQL: " + sql + where.toString());
      ps = database.getPreparedStatement(sql + where);
      rs = ps.executeQuery();
      List<MetaDatum> result = new ArrayList<MetaDatum>();
      while (rs.next()) {
        if (source.contains("rsam")) {
          md = new MetaDatum();
          md.cmid = rs.getInt(1);
          md.cid = rs.getInt(2);
          md.colid = rs.getInt(3);
          md.name = rs.getString(4);
          md.value = rs.getString(5);
          md.chName = rs.getString(6);
          md.colName = rs.getString(7);
          result.add(md);
        } else if (source.contains("hypocenters") | source.contains("lightning")) {
          md = new MetaDatum();
          md.cmid = rs.getInt(1);
          md.rid = rs.getInt(2);
          md.name = rs.getString(3);
          md.value = rs.getString(4);
          md.rkName = rs.getString(5);
          result.add(md);
        } else {
          md = new MetaDatum();
          md.cmid = rs.getInt(1);
          md.cid = rs.getInt(2);
          md.colid = rs.getInt(3);
          md.rid = rs.getInt(4);
          md.name = rs.getString(5);
          md.value = rs.getString(6);
          md.chName = rs.getString(7);
          md.colName = rs.getString(8);
          md.rkName = rs.getString(9);
          result.add(md);
        }
      }
      rs.close();
      return result;

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.getMatchingMetaData() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
      return null;
    }
  }

  /**
   * Process a getData request for metadata from this datasource.
   * 
   * @param params parameters for this request
   * @param cm = "is the name of the columns table coulmns_menu?"
   * @return RequestResult the desired meta data (null if an error occurred)
   */
  protected RequestResult getMetaData(Map<String, String> params, boolean cm) {
    String source = params.get("source");
    List<MetaDatum> data = null;
    MetaDatum metaDatum;
    List<String> result = new ArrayList<String>();

    String arg = params.get("byID");
    if (arg != null && arg.equals("true")) {
      metaDatum = new MetaDatum(-1, -1, -1);

      arg = params.get("ch");
      if (arg == null || arg.equals("") || source.contains("hypocenters") 
          || source.contains("lightning")) {
        metaDatum.cid = -1;
      } else {
        metaDatum.chName = arg;
        metaDatum.cid = 0;
      }

      arg = params.get("col");
      if (arg == null || arg.equals("")) {
        metaDatum.colid = -1;
      } else {
        metaDatum.colName = arg;
        metaDatum.colid = 0;
      }

      arg = params.get("rk");
      if (arg == null || arg.equals("") || source.contains("rsam")) {
        metaDatum.rid = -1;
      } else {
        metaDatum.rid = Integer.parseInt(arg);
      }
    } else {
      String chName = params.get("ch");
      String colName = params.get("col");
      String rkName = params.get("rk");
      metaDatum = new MetaDatum(chName, colName, rkName);
    }

    data = getMatchingMetaData(metaDatum, cm, source);
    if (data != null) {
      for (MetaDatum md : data) {
        result.add(String.format("%d,%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"", md.cmid, md.cid,
                   md.colid, md.rid, md.name, md.value, md.chName, md.colName, md.rkName));
      }
    }

    return new TextResult(result);
  }

  /**
   * Retrieve a collection of supplementary data.
   * 
   * @param sd the pattern to match (integers < 0 & null strings are ignored)
   * @param cm = "is the name of the columns table coulmns_menu?"
   * @return the desired supplementary data (null if an error occurred)
   */
  public List<SuppDatum> getMatchingSuppData(SuppDatum sd, boolean cm) {
    try {
      database.useDatabase(dbName);
      sql = "SELECT SD.sdid, SD.sdtypeid, SD.st, SD.et, SD.sd_short, SD.sd, CH.code, COL.name, "
          + "RK.name, ST.supp_data_type, ST.supp_color, SX.cid, SX.colid, SX.rid, ST.draw_line "
          + "FROM supp_data as SD, channels as CH, columns" + (cm ? "_menu" : "") + " as COL, "
          + "ranks as RK, supp_data_type as ST, supp_data_xref as SX "; // channelmetadata";
      String where = "WHERE SD.et >= " + sd.st + " AND SD.st <= " + sd.et + " AND SD.sdid = "
          + "SX.sdid AND SD.sdtypeid = ST.sdtypeid AND SX.cid = CH.cid AND SX.colid = COL.colid "
          + "AND SX.rid=RK.rid";

      if (sd.chName != null) {
        if (sd.cid < 0) {
          where = where + " AND CH.code='" + sd.chName + "'";
        } else {
          where = where + " AND CH.cid IN (" + sd.chName + ")";
        }
      } else if (sd.cid >= 0) {
        where = where + " AND SX.cid=" + sd.cid;
      }
      
      if (sd.colName != null) {
        if (sd.colid < 0) {
          where = where + " AND COL.name='" + sd.colName + "'";
        } else {
          where = where + " AND COL.colid IN (" + sd.colName + ")";
        }
      } else if (sd.colid >= 0) {
        where = where + " AND SX.colid=" + sd.colid;
      }
      
      if (sd.rkName != null) {
        if (sd.rid < 0) {
          where = where + " AND RK.name='" + sd.rkName + "'";
        } else {
          where = where + " AND RK.rid IN (" + sd.rkName + ")";
        }
      } else if (sd.rid >= 0) {
        where = where + " AND SX.rid=" + sd.rid;
      }
      
      if (sd.name != null) {
        where = where + " AND SD.sd_short=" + sd.name;
      }
      
      if (sd.value != null) {
        where = where + " AND SD.sd=" + sd.value;
      }
      
      String typeFilter = null;
      if (sd.typeName != null) {
        if (sd.typeName.length() == 0) {
          ;
        } else if (sd.tid == -1) {
          typeFilter = "ST.supp_data_type='" + sd.typeName + "'";
        } else {
          typeFilter = "ST.sdtypeid IN (" + sd.typeName + ")";
        }
      } else if (sd.tid >= 0) {
        typeFilter = "SD.sdtypeid=" + sd.tid;
      }
      
      if (sd.dl == -1) {
        if (typeFilter != null) {
          where = where + " AND " + typeFilter;
        }
      } else if (sd.dl < 2) {
        if (typeFilter != null) {
          where = where + " AND " + typeFilter;
        }
        where = where + " AND ST.dl='" + sd.dl;
      } else if (typeFilter != null) {
        where = where + " AND (" + typeFilter + " OR ST.draw_line='0')";
      } else {
        where = where + " AND ST.draw_line='0'";
      }

      // logger.info( "SQL: " + sql + where );
      ps = database.getPreparedStatement(sql + where);
      rs = ps.executeQuery();
      List<SuppDatum> result = new ArrayList<SuppDatum>();
      while (rs.next()) {
        result.add(new SuppDatum(rs));
      }
      rs.close();
      return result;

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.getMatchingSuppData() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
      return null;
    }
  }

  /**
   * Insert a piece of supplemental data.
   * 
   * @param sd the SuppDatum to be added
   * @return ID of the record, -ID if already present, 0 if failed
   */
  public int insertSuppDatum(SuppDatum sd) {
    try {
      database.useDatabase(dbName);

      sql = "INSERT INTO supp_data (sdtypeid,st,et,sd_short,sd) VALUES (" + sd.tid + "," + sd.st
          + "," + sd.et + ",\"" + sd.name + "\",\"" + sd.value + "\")";

      ps = database.getPreparedStatement(sql);

      ps.execute();

      rs = ps.getGeneratedKeys();

      rs.next();

      return rs.getInt(1);
    } catch (SQLException e) {
      if (!e.getSQLState().equals("23000")) {
        logger.log(Level.SEVERE, "SQLDataSource.insertSuppDatum() failed. ("
            + database.getDatabasePrefix() + "_" + dbName + ")", e);
        return 0;
      }
    }
    
    try {
      sql = "SELECT sdid FROM supp_data WHERE sdtypeid=" + sd.tid + " AND st=" + sd.st + " AND et="
          + sd.et + " AND sd_short='" + sd.name + "'";
      ps = database.getPreparedStatement(sql);

      rs = ps.executeQuery();

      rs.next();

      return -rs.getInt(1);
    } catch (SQLException e2) {
      logger.log(Level.SEVERE, "SQLDataSource.insertSuppDatum() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e2);
      return 0;
    }
  }

  /**
   * Update a piece of supplemental data.
   * 
   * @param sd the SuppDatum to be added
   * @return ID of the record, 0 if failed
   */
  public int updateSuppDatum(SuppDatum sd) {
    try {
      database.useDatabase(dbName);

      sql = "UPDATE supp_data SET sdtypeid='" + sd.tid + "',st='" + sd.st + "',et='" + sd.et
          + "',sd_short='" + sd.name + "',sd='" + sd.value + "' WHERE sdid='" + sd.sdid + "'";

      ps = database.getPreparedStatement(sql);

      ps.execute();

      return sd.sdid;
    } catch (SQLException e) {
      logger.log(Level.SEVERE, "SQLDataSource.updateSuppDatum() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
      return 0;
    }
  }

  /**
   * Insert a supplemental data xref.
   * 
   * @param sd the SuppDatum xref to be added
   * @return true if successful, false otherwise
   */
  public boolean insertSuppDatumXref(SuppDatum sd) {
    try {
      database.useDatabase(dbName);
      sql = "INSERT INTO supp_data_xref (sdid, cid, colid, rid) VALUES (" + sd.sdid + "," + sd.cid
          + "," + sd.colid + "," + sd.rid + ");";

      ps = database.getPreparedStatement(sql);

      ps.execute();
    } catch (SQLException e) {
      if (!e.getSQLState().equals("23000")) {
        logger.log(Level.SEVERE, "SQLDataSource.insertSuppDatumXref() failed. ("
            + database.getDatabasePrefix() + "_" + dbName + ")", e);
        return false;
      }
      logger.info("SQLDataSource.insertSuppDatumXref: SDID " + sd.sdid
          + " xref already exists for given parameters");
    }
    return true;
  }

  /**
   * Insert a supplemental datatype.
   * 
   * @param sd the datatype to be added
   * @return ID of the datatype, -ID if already present, 0 if failed
   */
  public int insertSuppDataType(SuppDatum sd) {
    try {
      database.useDatabase(dbName);

      sql = "INSERT INTO supp_data_type (supp_data_type,supp_color,draw_line) VALUES (" + "\""
          + sd.typeName + "\",\"" + sd.color + "\"," + sd.dl + ");";

      ps = database.getPreparedStatement(sql);

      ps.execute();

      rs = ps.getGeneratedKeys();

      rs.next();

      return rs.getInt(1);
    } catch (SQLException e) {
      if (!e.getSQLState().equals("23000")) {
        logger.log(Level.SEVERE, "SQLDataSource.insertSuppDataType() failed. ("
            + database.getDatabasePrefix() + "_" + dbName + ")", e);
        return 0;
      }
    }
    
    try {
      sql = "SELECT sdid FROM supp_data WHERE sdtypeid=" + sd.tid + " AND st=" + sd.st + " AND et="
          + sd.et + " AND sd_short='" + sd.name + "'";
      ps = database.getPreparedStatement(sql);
      rs = ps.executeQuery();
      rs.next();
      return -rs.getInt(1);

    } catch (SQLException e) {
      logger.log(Level.SEVERE, "SQLDataSource.insertSuppDataType() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
      return 0;
    }
  }

  /**
   * Process a getData request for supplementary data from this datasource.
   * 
   * @param params parameters for this request
   * @param cm = "is the name of the columns table coulmns_menu?"
   * @return RequestResult the desired supplementary data (null if an error occurred)
   */
  protected RequestResult getSuppData(Map<String, String> params, boolean cm) {
    double st;
    double et;
    String arg = null;
    SuppDatum suppDatum;

    String tz = params.get("tz");
    if (tz == null || tz.equals("")) {
      tz = "UTC";
    }
    SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    df.setTimeZone(TimeZone.getTimeZone(tz));

    try {
      arg = params.get("st");
      st = Util.dateToJ2K(df.parse(arg));
      arg = params.get("et");
      if (arg == null || arg.equals("")) {
        et = Double.MAX_VALUE;
      } else {
        et = Util.dateToJ2K(df.parse(arg));
      }
    } catch (Exception e) {
      return getErrorResult("Illegal time string: " + arg + ", " + e);
    }

    arg = params.get("byID");
    if (arg != null && arg.equals("true")) {
      suppDatum = new SuppDatum(st, et, -1, -1, -1, -1);
      String[] args = {"ch", "col", "rk", "type"};
      for (int i = 0; i < 4; i++) {
        arg = params.get(args[i]);
        if (arg == null || arg.equals("")) {
          args[i] = null;
        } else {
          args[i] = arg;
        }
      }
      suppDatum.chName = args[0];
      if (suppDatum.chName != null) {
        suppDatum.cid = 0;
      }
      
      suppDatum.colName = args[1];
      if (suppDatum.colName != null) {
        suppDatum.colid = 0;
      }
      
      suppDatum.rkName = args[2];
      if (suppDatum.rkName != null) {
        suppDatum.rid = 0;
      }
      
      suppDatum.typeName = args[3];
      if (suppDatum.typeName != null) {
        suppDatum.tid = 0;
      }
    } else {
      String chName = params.get("ch");
      String colName = params.get("col");
      String rkName = params.get("rk");
      String typeName = params.get("type");
      suppDatum = new SuppDatum(st, et, chName, colName, rkName, typeName);
    }
    
    arg = params.get("dl");
    if (arg != null) {
      suppDatum.dl = Integer.parseInt(arg);
    }
    
    List<SuppDatum> data = null;
    data = getMatchingSuppData(suppDatum, cm);
    if (data != null) {
      List<String> result = new ArrayList<String>();
      for (SuppDatum sd : data) {
        result.add(String.format(
            "%d,%1.3f,%1.3f,%d,%d,%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
            sd.sdid, sd.st, sd.et, sd.cid, sd.tid, sd.colid, sd.rid, sd.dl, sd.name,
            sd.value.replace('\n', Character.MIN_VALUE), sd.chName, sd.typeName, sd.colName,
            sd.rkName, sd.color));
      }
      return new TextResult(result);
    }
    return null;
  }

  /**
   * Retrieve the collection of supplementary data types.
   * 
   * @return the desired supplementary data types (null if an error occurred)
   */
  public List<SuppDatum> getSuppDataTypes() {

    List<SuppDatum> types = new ArrayList<SuppDatum>();
    try {
      database.useDatabase(dbName);
      sql = "SELECT sdtypeid, supp_data_type, supp_color, draw_line FROM supp_data_type";
      ps = database.getPreparedStatement(sql);
      rs = ps.executeQuery();
      while (rs.next()) {
        SuppDatum sd = new SuppDatum(0.0, 0.0, -1, -1, -1, rs.getInt(1));
        sd.typeName = rs.getString(2);
        sd.color = rs.getString(3);
        sd.dl = rs.getInt(4);
        types.add(sd);
      }
      rs.close();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.getSuppDataTypes() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
      return null;
    }

    return types;
  }

  /**
   * Get supp data types list in format 'sdtypeid"draw_line"name"color' from database.
   * 
   * @param drawOnly yield only the drawable types
   * @return List of Strings with " separated values
   */
  public RequestResult getSuppTypes(boolean drawOnly) {
    List<String> result = new ArrayList<String>();

    try {
      database.useDatabase(dbName);
      sql = "SELECT sdtypeid, supp_data_type, supp_color, draw_line FROM supp_data_type";
      if (drawOnly) {
        sql = sql + " WHERE draw_line=1";
      }
      rs = database.getPreparedStatement(sql + " ORDER BY supp_data_type").executeQuery();
      while (rs.next()) {
        result.add(String.format("%d\"%d\"%s\"%s", rs.getInt(1), rs.getInt(4), rs.getString(2),
            rs.getString(3)));
      }
      rs.close();

    } catch (Exception e) {
      logger.log(Level.SEVERE, "SQLDataSource.defaultGetSuppdataTypes() failed. ("
          + database.getDatabasePrefix() + "_" + dbName + ")", e);
    }

    return new TextResult(result);
  }
}
