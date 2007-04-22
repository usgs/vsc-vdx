package gov.usgs.vdx.db;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.Retriable;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.generic.SQLGenericDataSource;
import gov.usgs.vdx.data.gps.SQLGPSDataSource;
import gov.usgs.vdx.data.hypo.SQLHypocenterDataSource;
import gov.usgs.vdx.data.nwis.SQLNWISDataSource;
import gov.usgs.vdx.data.rsam.SQLEWRSAMDataSource;
import gov.usgs.vdx.data.tilt.SQLElectronicTiltDataSource;
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
 * Not thread-safe.
 * 
 * TODO: refactor so VDXDatabase and WinstonDatabase derive from a common source.
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.7  2006/08/01 17:52:35  tparker
 * Add tableExist method
 *
 * Revision 1.6  2006/04/09 18:26:05  dcervelli
 * ConfigFile/type safety changes.
 *
 * Revision 1.5  2005/10/20 18:00:48  dcervelli
 * Added creategeneric.
 *
 * Revision 1.4  2005/10/13 22:17:13  dcervelli
 * Added etilt.
 *
 * Revision 1.3  2005/09/01 00:29:26  dcervelli
 * Added tilt database creation.
 *
 * Revision 1.2  2005/08/29 15:56:04  dcervelli
 * New logging changes.
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class VDXDatabase
{
	private static final String DEFAULT_CONFIG_FILE = "VDX.config";
	private static final String CURRENT_SCHEMA_VERSION = "1.0.0";
	private static final String DEFAULT_DATABASE_PREFIX = "V";
	
	private Connection connection;
	private Statement statement;

	private boolean connected;

	private String dbDriver;
	private String dbURL;
	
	private String databasePrefix = DEFAULT_DATABASE_PREFIX;
	
	private Logger logger;
	
	private Map<String, PreparedStatement> preparedStatements;
	
	public VDXDatabase(String driver, String url, String db)
	{
	    logger = Log.getLogger("gov.usgs.vdx");
		dbDriver = driver;
		dbURL = url;
		if (db != null)
			databasePrefix = db;
		preparedStatements = new HashMap<String, PreparedStatement>();
		connect();
	}
	
	public static VDXDatabase getVDXDatabase(String cf)
	{
		VDXDatabase db = null;
		try
		{
			ConfigFile config = new ConfigFile(cf);
			String driver = config.getString("vdx.driver");
			String url = config.getString("vdx.url");
			String prefix = config.getString("vdx.prefix");
			db = new VDXDatabase(driver, url, prefix);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return db;
	}
	
	public Logger getLogger()
	{
	    return logger;
	}
	
	public void setLogger(Logger log)
	{
	    logger = log;
	}
	
	public void connect()
	{
		connected = false;
		try
		{
			Class.forName(dbDriver).newInstance();
			DriverManager.setLoginTimeout(3);
			connection = DriverManager.getConnection(dbURL);
			statement = connection.createStatement();
			connected = true;
			preparedStatements.clear();
		}
		catch (ClassNotFoundException e)
		{
		    logger.log(Level.SEVERE, "Could not load the database driver, check your CLASSPATH.", Util.getLineNumber(this, e));
		    System.exit(-1);
		}
		catch (Exception e)
		{
			connection = null;
			statement = null;
			logger.log(Level.SEVERE, "Could not connect to VDX.", e);
			connected = false;
		}	
	}
	
	public void close()
	{
		if (!checkConnect())
			return;
			
		try
		{
			statement.close();
			connection.close();
			connected = false;
		}
		catch (Exception e)
		{
			logger.warning("Error closing database.  This is unusual, but not critical.");
		}
	}

	public boolean checkConnect()
	{
		return checkConnect(true);
	}
	
	public boolean checkConnect(final boolean verbose)
	{
		if (connected)
			return true;
		else
		{
			new Retriable<Object>()
					{
						public boolean attempt()
						{
							connect();
							return connected;
						}
					}.go();
			return connected;
		}
	}

	public boolean connected()
	{
		return connected;
	}
	
	public Connection getConnection()
	{
		return connection;
	}
	
	public Statement getStatement()
	{
		return statement;	
	}

	public boolean execute(final String sql)
	{
		Boolean b = new Retriable<Boolean>()
		{
			public void attemptFix()
			{
				close();
				connect();
			}
	
			public boolean attempt()
			{
				try
				{
					statement.execute(sql);
					result = new Boolean(true);
					return true;
				}
				catch (SQLException e)
				{
				    logger.log(Level.SEVERE, "execute() failed, SQL: " + sql, e);
				}
				result = new Boolean(false);
				return false;
			}
		}.go();
	
		return b != null && b.booleanValue();
	}
	
	public ResultSet executeQuery(final String sql)
	{
		ResultSet rs = new Retriable<ResultSet>()
		{
			public void attemptFix()
			{
				close();
				connect();
			}
	
			public boolean attempt()
			{
				try
				{
					result = statement.executeQuery(sql);
					return true;
				}
				catch (SQLException e)
				{
					logger.log(Level.SEVERE, "executeQuery() failed, SQL: " + sql, e);
				}
				return false;
			}
		}.go();
		
		return rs;
	}
	
	public String getDatabasePrefix()
	{
	    return databasePrefix;
	}
	
	private void createTables()
	{
		try
		{
			useRootDatabase();
		    getStatement().execute("CREATE TABLE version (schemaversion VARCHAR(10), installtime DATETIME)");
		    getStatement().execute("INSERT INTO version VALUES ('" + CURRENT_SCHEMA_VERSION + "', NOW())");
		}
		catch (Exception e)
		{
		    logger.log(Level.SEVERE, "Could not create table in VDX database.  Are permissions set properly?", Util.getLineNumber(this, e));
		}
	}
	
	public boolean useRootDatabase()
	{
		return useDatabase("ROOT");
	}
	
	public boolean useDatabase(String db)
	{
		if (!checkConnect())
			return false;
		
		try
		{
			try
			{
				statement.execute("USE " + databasePrefix + "_" + db);
			}
			catch (SQLException e)
			{
				logger.log(Level.SEVERE, "Lost connection to database, attempting to reconnect.");
				close();
				connect();
			}
			statement.execute("USE " + databasePrefix + "_" + db);
			return true;
		}
		catch (SQLException e)
		{
			if (e.getMessage().indexOf("Unknown database") != -1)
				logger.log(Level.SEVERE, "Attempt to use nonexistent database: " + db);
			else
				logger.log(Level.SEVERE, "Could not use database: " + db, e);
		}
		return false;
	}
	
	public boolean checkDatabase()
	{
		if (!checkConnect())
			return false;
			
		try
		{
		    boolean failed = false;
		    try
		    {
		        getStatement().execute("USE " + getDatabasePrefix() + "_ROOT");
		    }
		    catch (Exception e)
		    {
		        failed = true;
		    }
		    if (failed)
		    {
		        getStatement().execute("CREATE DATABASE " + getDatabasePrefix() + "_ROOT");
		        getStatement().execute("USE " + getDatabasePrefix() + "_ROOT");
		        createTables();
		    }
			return true;
		}
		catch (Exception e)
		{
			logger.severe("Could not locate or create VDX database.  Are permissions set properly?");
		}	
		return false;
	}
	
	public PreparedStatement getPreparedStatement(String sql)
	{
		try
		{
			PreparedStatement ps = preparedStatements.get(sql);
			if (ps == null)
			{
				ps = connection.prepareStatement(sql);
				preparedStatements.put(sql, ps);
			}
			return ps;
		}
		catch (Exception e)
		{
			logger.log(Level.SEVERE, "Could not prepare statement.", e);
		}
		return null;
	}

	protected static void outputInstructions()
	{
		System.out.println("<VDXDatabase> [-c configfile] -a <action> [other args]");
	}
	
	protected static void createDatabase(VDXDatabase db, Arguments args, SQLDataSource ds)
	{
		String name = args.get("-n");
		if (name == null)
		{
			System.err.println("You must specify the name of the database with '-n'.");
			System.exit(-1);
		}
		ds.setDatabase(db);
		ds.setName(name);
		boolean success = ds.createDatabase();
		String msg = success ? "Successfully created database." : "Failed to create database.";
		System.out.println(msg);
	}
	
	public boolean tableExists(String db, String table)
	{
		try
		{
			ResultSet rs = getStatement().executeQuery(
					String.format("SELECT COUNT(*) FROM %s_%s.%s", databasePrefix, db, table));
			boolean result = rs.next();
			rs.close();
			return result;
		}
		catch (Exception e)
		{}
		return false;
	}	
	
	public static void main(String[] as)
	{
		Set<String> flags = new HashSet<String>();
		Set<String> kvs = new HashSet<String>();
		kvs.add("-c");
		kvs.add("-n");
		kvs.add("-a");
		Arguments args = new Arguments(as, flags, kvs);
		String cf = args.get("-c");
		if (cf == null)
			cf = DEFAULT_CONFIG_FILE;
		
		VDXDatabase db = VDXDatabase.getVDXDatabase(cf);
		if (db == null)
		{
			System.out.println("Could not connect to VDX database");
			System.exit(-1);
		}
		
		String action = args.get("-a");
		if (action == null)
			outputInstructions();
		else
		{
			action = action.toLowerCase();
			if (action.equals("createvdx"))
				db.checkDatabase();
			else
			{
				Map<String, SQLDataSource> sources = new HashMap<String, SQLDataSource>();
				sources.put("createhypocenters", new SQLHypocenterDataSource());
				sources.put("creategps", new SQLGPSDataSource());
				sources.put("createtilt", new SQLTiltDataSource());
				sources.put("createetilt", new SQLElectronicTiltDataSource());
				sources.put("creategeneric", new SQLGenericDataSource());
				sources.put("createnwis", new SQLNWISDataSource());
				sources.put("createewrsam", new SQLEWRSAMDataSource());
				SQLDataSource sds = sources.get(action);
				if (sds != null)
					createDatabase(db, args, sds);
				else
					System.out.println("I don't know how to " + action);
					
			}
		}
	}
}
