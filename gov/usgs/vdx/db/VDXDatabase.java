package gov.usgs.vdx.db;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.Retriable;
import gov.usgs.util.Util;
import gov.usgs.util.UtilException;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.generic.fixed.SQLGenericFixedDataSource;
import gov.usgs.vdx.data.generic.variable.SQLGenericVariableDataSource;
import gov.usgs.vdx.data.gps.SQLGPSDataSource;
import gov.usgs.vdx.data.hypo.SQLHypocenterDataSource;
import gov.usgs.vdx.data.rsam.SQLRSAMDataSource;
import gov.usgs.vdx.data.rsam.SQLEWRSAMDataSource;
import gov.usgs.vdx.data.tensorstrain.SQLTensorstrainDataSource;
import gov.usgs.vdx.data.tilt.SQLTiltDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Keeps SQL database-related information needed to make connection.
 * 
 * Not thread-safe.
 * 
 * TODO: refactor so VDXDatabase and WinstonDatabase derive from a common
 * source.
 * 
 * 
 * @author Dan Cervelli
 */
public class VDXDatabase {
	private static final String DEFAULT_CONFIG_FILE = "VDX.config";
	private static final String CURRENT_SCHEMA_VERSION = "1.0.0";
	private static final String DEFAULT_DATABASE_PREFIX = "V";

	private Connection connection;
	private Statement statement;

	private boolean connected;

	private String dbDriver;
	private String dbURL;

	private String dbPrefix = DEFAULT_DATABASE_PREFIX;

	private Logger logger;

	private Map<String, PreparedStatement> preparedStatements;

	/**
	 * Constructor
	 * 
	 * @param driver
	 *            class name for database driver
	 * @param url
	 *            database url
	 * @param prefix
	 *            database prefix
	 */
	public VDXDatabase(String driver, String url, String prefix) {
		logger = Log.getLogger("gov.usgs.vdx");
		logger.finest("New VDXDatabase: " + driver + ":" + url + ":" + prefix);
		dbDriver = driver;
		dbURL = url;
		if (prefix != null)
			dbPrefix = prefix;
		preparedStatements = new HashMap<String, PreparedStatement>();
		connect();
	}

	/**
	 * Construct VDXdatabase from configuration
	 * 
	 * @param cf
	 *            content of configuration file
	 */
	public static VDXDatabase getVDXDatabase(String cf) {
		VDXDatabase db = null;
		try {
			ConfigFile config	= new ConfigFile(cf);
			String driver		= config.getString("vdx.driver");
			String url			= config.getString("vdx.url");
			String prefix		= config.getString("vdx.prefix");
			
			if (driver == null)
				throw new RuntimeException("Can't find config parameter vdx.driver.");
			if (url == null)
				throw new RuntimeException("Can't find config parameter vdx.url.");
			if (prefix == null)
				throw new RuntimeException("Can't find config parameter vdx.prefix.");
			
			db = new VDXDatabase(driver, url, prefix);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return db;
	}

	/**
	 * Getter for logger
	 * @return logger
	 */
	public Logger getLogger() {
		return logger;
	}

	/**
	 * Setter for logger
	 * @param log logger
	 */
	public void setLogger(Logger log) {
		logger = log;
	}

	/**
	 * Performs database connection
	 */
	public void connect() {
		logger.fine("Connecting to " + dbURL);
		connected = false;
		try {
			Class.forName(dbDriver).newInstance();
			DriverManager.setLoginTimeout(3);
			connection = DriverManager.getConnection(dbURL);
			statement = connection.createStatement();
			connected = true;
			preparedStatements.clear();
		} catch (ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Could not load the database driver, check your CLASSPATH.", Util.getLineNumber(this, e));
			System.exit(-1);
		} catch (Exception e) {
			connection = null;
			statement = null;
			logger.log(Level.SEVERE, "Could not connect to VDX.", e);
			connected = false;
		}
	}

	/**
	 * Close database connection
	 */
	public void close() {
		if (!checkConnect())
			return;

		try {
			statement.close();
			connection.close();
			connected = false;
		} catch (Exception e) {
			logger.warning("Error closing database.  This is unusual, but not critical.");
		}
	}

	/**
	 * Make connection if it was closed
	 * 
	 * @return true if connected
	 */
	public boolean checkConnect() {
		return checkConnect(true);
	}

	/**
	 * Make connection if it was closed
	 * 
	 * @return true if connected
	 */
	public boolean checkConnect(final boolean verbose) {
		if (connected)
			return true;
		else {
			try{
				new Retriable<Object>() {
					public boolean attempt() throws UtilException {
						connect();
						return connected;
					}
				}.go();
			}
			catch(UtilException e){
				//Do nothing 
			}
			return connected;
		}
	}

	/**
	 * Check if connection active
	 * @return true if connected
	 */
	public boolean connected() {
		return connected;
	}

	/**
	 * Getter for database connection
	 * @return connection
	 */
	public Connection getConnection() {
		return connection;
	}

	/**
	 * Getter for statement
	 * @return statement
	 */
	public Statement getStatement() {
		return statement;
	}

	/**
	 * Execute given sql
	 * 
	 * @param sql the sql to execute
	 * @return true if success
	 */
	public boolean execute(final String sql) {
		Boolean b = null;
		try{
			b = new Retriable<Boolean>() {
				public void attemptFix() {
					close();
					connect();
				}
				public boolean attempt() throws UtilException {
					try {
						statement.execute(sql);
						result = new Boolean(true);
						return true;
					} catch (SQLException e) {
						logger.log(Level.SEVERE, "execute() failed, SQL: " + sql, e);
					}
					result = new Boolean(false);
					return false;
				}
			}.go();
		}
		catch(UtilException e){
			//Do nothing 
		}
		return b != null && b.booleanValue();
	}

	/**
	 * Execute given sql
	 * 
	 * @param sql  query to execute
	 * @return result set given from database
	 */
	public ResultSet executeQuery(final String sql) {
		ResultSet rs = null;
		try{
			rs = new Retriable<ResultSet>() {
				public void attemptFix() {
					close();
					connect();
				}
				public boolean attempt() {
					try {
						result = statement.executeQuery(sql);
						return true;
					} catch (SQLException e) {
						logger.log(Level.SEVERE, "executeQuery() failed, SQL: " + sql, e);
					}
					return false;
				}
			}.go();
		}
		catch(UtilException e){
			//Do nothing 
		}
		return rs;
	}

	/**
	 * Getter for VDX database prefix
	 * @return database prefix
	 */
	public String getDatabasePrefix() {
		return dbPrefix;
	}

	/**
	 * Create 'version' table and insert current values
	 */
	private void createTables() {
		try {
			useRootDatabase();
			getStatement().execute("CREATE TABLE version (schemaversion VARCHAR(10), installtime DATETIME)");
			getStatement().execute("INSERT INTO version VALUES ('" + CURRENT_SCHEMA_VERSION + "', NOW())");
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Could not create table in VDX database.  Are permissions set properly?", Util.getLineNumber(this, e));
		}
	}

	/**
	 * Select 'ROOT' database to use inside SQL server
	 * 
	 * @return true if success
	 */
	public boolean useRootDatabase() {
		return useDatabase("ROOT");
	}

	/**
	 * Select database to use inside SQL server
	 * 
	 * @param db
	 *            database name (without prefix)
	 * @return true if success
	 */
	public boolean useDatabase(String db) {
		db = dbPrefix + "_" + db;
		if (!checkConnect())
			return false;

		try {
			/*
			 * try { statement.execute("USE " + db); } catch (SQLException e) {
			 * logger.log(Level.SEVERE, "Could not connect to " + db +
			 * ", attempting to reconnect ..."); close(); connect(); }
			 */
			statement.execute("USE " + db);
			return true;
		} catch (SQLException e) {
			if (e.getMessage().indexOf("Unknown database") != -1)
				logger.log(Level.SEVERE, db + " database does not exist");
			else
				logger.log(Level.SEVERE, db + " database connection failed", e);
		}
		return false;
	}

	/**
	 * Select VALVE 2 database to use inside SQL server
	 * 
	 * @param db
	 *            database name (without prefix)
	 * @return true if success
	 */
	public boolean useV2Database(String db) {
		if (!checkConnect())
			return false;

		try {
			try {
				statement.execute("USE " + db);
			} catch (SQLException e) {
				logger.log(Level.SEVERE, "Lost connection to VALVE 2 database, attempting to reconnect.");
				close();
				connect();
			}
			statement.execute("USE " + db);
			return true;
		} catch (SQLException e) {
			if (e.getMessage().indexOf("Unknown database") != -1)
				logger.log(Level.SEVERE, "Attempt to use nonexistent database: " + db);
			else
				logger.log(Level.SEVERE, "Could not use database: " + db, e);
		}
		return false;
	}

	/**
	 * Create database 'ROOT' if it isn't exist and use it.
	 * 
	 * @return true if success
	 */
	public boolean checkDatabase() {
		if (!checkConnect())
			return false;

		try {
			boolean failed = false;
			try {
				getStatement().execute("USE " + getDatabasePrefix() + "_ROOT");
			} catch (Exception e) {
				failed = true;
			}
			if (failed) {
				getStatement().execute("CREATE DATABASE " + getDatabasePrefix() + "_ROOT");
				getStatement().execute("USE " + getDatabasePrefix() + "_ROOT");
				createTables();
			}
			return true;
		} catch (Exception e) {
			logger.severe("Could not locate or create VDX database.  Are permissions set properly?");
		}
		return false;
	}

	/**
	 * Prepare statement for sql
	 * 
	 * @param sql statement to prepare
	 * @return prepared statement
	 */
	public PreparedStatement getPreparedStatement(String sql) {
		try {
			PreparedStatement ps = preparedStatements.get(sql);
			if (ps == null) {
				ps = connection.prepareStatement(sql);
				preparedStatements.put(sql, ps);
			}
			return ps;
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Could not prepare statement.", e);
		}
		return null;
	}

	/**
	 * Create given VDX database
	 * 
	 * @param db
	 *            VDX database
	 * @param args
	 *            command line arguments
	 * @param ds
	 *            data source
	 */
	protected static void createDatabase(ConfigFile params, Arguments args, SQLDataSource ds) {
		String name = args.get("-n");
		if (name == null) {
			System.err.println("You must specify the name of the database with '-n'.");
			System.exit(-1);
		}
		params.put("vdx.name", name);
		ds.initialize(params);
		// ds.createDatabase(); (not necessary.  initialize will create the database if it does not exist)
	}

	/**
	 * Check if table exist in the database
	 * 
	 * @param db
	 *            database name
	 * @param table
	 *            table name
	 * @return    true if table exists
	 */
	public boolean tableExists(String db, String table) {
		try {
			ResultSet rs = getStatement().executeQuery(String.format("SELECT COUNT(*) FROM %s_%s.%s", dbPrefix, db, table));
			boolean result = rs.next();
			rs.close();
			return result;
		} catch (Exception e) {
		}
		return false;
	}

	/**
	 * Main method, provide command-line interface
	 * @param as commend line args
	 */
	public static void main(String[] as) {

		Set<String> flags = new HashSet<String>();
		Set<String> kvs = new HashSet<String>();
		kvs.add("-c");
		kvs.add("-n");
		kvs.add("-a");
		Arguments args = new Arguments(as, flags, kvs);

		String cf = args.get("-c");
		if (cf == null) {
			cf = DEFAULT_CONFIG_FILE;
		}
		
		ConfigFile params = new ConfigFile(cf);

		VDXDatabase db = VDXDatabase.getVDXDatabase(cf);
		if (db == null) {
			System.out.println("Could not connect to VDX database");
			System.exit(-1);
		}

		String action = args.get("-a");
		if (action == null) {
			System.out.println("<VDXDatabase> [-c configfile] -a <action> [other args]");
			System.out.println("Known actions:");
			System.out.println("creategenericfixed");
			System.out.println("creategenericvariable");
			System.out.println("creategps");
			System.out.println("createhypocenters");
			System.out.println("createewrsam");
			System.out.println("createtilt");
			System.out.println("createtensorstrain");
		} else {
			action = action.toLowerCase();
			if (action.equals("createvdx")) {
				db.checkDatabase();
			} else {
				Map<String, SQLDataSource> sources = new HashMap<String, SQLDataSource>();
				sources.put("creategenericfixed", new SQLGenericFixedDataSource());
				sources.put("creategenericvariable", new SQLGenericVariableDataSource());
				sources.put("creategps", new SQLGPSDataSource());
				sources.put("createhypocenters", new SQLHypocenterDataSource());
				sources.put("creatersam", new SQLRSAMDataSource());
				sources.put("createewrsam", new SQLEWRSAMDataSource());
				sources.put("createtilt", new SQLTiltDataSource());
				sources.put("createtensorstrain", new SQLTensorstrainDataSource());
				SQLDataSource sds = sources.get(action);
				if (sds != null) {
					createDatabase(params, args, sds);
				} else {
					System.out.println("I don't know how to " + action);
				}
			}
		}
	}
}
