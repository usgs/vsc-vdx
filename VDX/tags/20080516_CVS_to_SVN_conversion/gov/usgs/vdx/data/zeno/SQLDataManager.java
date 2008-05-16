package gov.usgs.vdx.data.zeno;

import java.sql.*;
import java.lang.reflect.*;

/**
 * <p>This is a DataManager that talks to an SQL database.  This class is specified
 * abstract because there is no reason it should be instantiated not because there
 * are abstract functions.
 *
 * <p>Built in is a retry/reset mechanism that can be used to attempt to restore
 * crashed/bad connections to the database.
 *
 * <p>SQLDataManagers are singleton objects.  Only a single connection to the database
 * exists for each one (SQLTiltDataManager, SQLCatalogDataManager, etc.) and all
 * requests are funneled through that one connection.  In practice this is not
 * a performance bottleneck because of the limited number of simultaneous users Valve should
 * be subject to. Also important to note, all functions in subclasses that access
 * the database should be synchronized.
 *
 * <p>Of course, for a larger (public?) project you would probably want to reimplement
 * this class with some sort of connection pool.
 *
 * $Log: not supported by cvs2svn $
 * 
 * @@author  Dan Cervelli
 * @@version 2.00
 */
public abstract class SQLDataManager extends DataManager
{
	/** a flag that specifies if this SQLDataManager is currently retrying a function 
	 */
	protected boolean inRetry = false;
	
	/** the JDBC database driver fully-qualified class name 
	 */
    protected String driver;
	
	/** the driver-specific URL to use to connect to the database
	 */
    protected String url;
	
	/** a connection to the database
	 */
    private Connection connection;
	
	/** a single statement through which most/all requests will be made
	 */ 
    private Statement statement;
    
	/** generic empty constructor.
	 */
    protected SQLDataManager() {}

	/** Attempts to connect to the database.
	 */
    public void connect()
    {
        try
        {
            Class.forName(driver).newInstance();
            connection = DriverManager.getConnection(url);
            statement = connection.createStatement();
        }
        catch (Exception e)
        {
			connection = null;
			statement = null;
			// Valve.getLogger().warning("Exception: " + e.getMessage() + "\nCould not connect to database.");
			System.err.println("Exception: " + e.getMessage() + "\nCould not connect to database.");
        }
    }

	/** Attempts to connect to the database but does not call the driver instantiation first.
	 */
	private void connectNoNewDriver()
	{
        try
        {
            connection = DriverManager.getConnection(url);
            statement = connection.createStatement();
        }
        catch (Exception e)
        {
			connection = null;
			statement = null;
			// Valve.getLogger().warning("Exception: " + e.getMessage() + "\nCould not connect to database.");
			System.err.println("Exception: " + e.getMessage() + "\nCould not connect to database.");
        }
	}
	
	/** Gets a statement through which a query can be executed.
	 * @@return the statement
	 */
	public Statement getStatement()
	{
		return statement;
	}
	
	/** Gets a new statement. This can be used if a certain SQLDataManager wants
	 * more than one statement for whatever reason (nested queries perhaps).
	 * @@return a new statement
	 */
	public Statement getNewStatement()
	{
		Statement result = null;
		try
		{
			result = connection.createStatement();
		}
		catch (SQLException e)
		{
			// Valve.getLogger().warning("Exception: " + e.getMessage() + "\nCould not create statement.");
			System.err.println("Exception: " + e.getMessage() + "\nCould not create statement.");
		}
		return result;
	}
	
	/** Closes the connection to the database.
	 */
    public void close()
    {
        try
        {
            connection.close();
            statement.close();
        }
        catch (Exception e)
        {
			// Valve.getLogger().warning("Exception: " + e.getMessage() + "\nCould not close database.");
			System.err.println("Exception: " + e.getMessage() + "\nCould not close database.");
            //e.printStackTrace();
        }
    }
	
	/** Resets (closes/reopens) the connection to the database.
	 */
	public void reset()
	{
		close();
		connectNoNewDriver();
	}
	
	/** Retrys a method that accesses the database.  This first resets the connection
	 * to the database and then calls the specified method with the specified arguments.
	 * This is implemented here (as opposed to inside a particular SQLDataManager
	 * to provided a non-recursive system (and all of the accompanying overhead)
	 * for retrying database functions.
	 *
	 * @@param methodName the name of the method to call
	 * @@param margs the Class types of the method arguments (ex: new Class[] {String.Class, Double.TYPE, Double.TYPE})
	 * @@param invoker the object that will call the method
	 * @@param args the arguments to the method (primitives should be wrapped)
	 * @@return an object that should be cast to the appropriate return type (and unwrapped if primitive)
	 */
	public Object retry(String methodName, Class[] margs, Object invoker, Object[] args)
	{
		inRetry = true;
		Object result = null;
		try
		{
			reset();
			if (methodName != null && connection != null && statement != null)
			{
				Method method = invoker.getClass().getMethod(methodName, margs);
				result = method.invoke(invoker, args);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		inRetry = false;
		return result;
	}
}