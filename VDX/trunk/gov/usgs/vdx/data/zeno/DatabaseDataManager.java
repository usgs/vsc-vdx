package gov.usgs.vdx.data.zeno;

import java.sql.*;

/**
 * Keeps information needed to initialize database connection and connection itself.
 * 
 * @author  cervelli
 */
abstract public class DatabaseDataManager 
{
    protected String driver;
    protected String urlAddress;
    protected String databaseName;
    protected String parameters;
    protected Connection connection;
    protected Statement statement;
    
    /**
     * Default constructor
     */
    protected DatabaseDataManager() {}
   
    /**
     * Perform connection to database.
     */
    public void connect()
    {
        try
        {
            Class.forName(driver).newInstance();
            connection = DriverManager.getConnection(urlAddress + databaseName + parameters);
            statement = connection.createStatement();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
 
    /**
     * Close connection to database.
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
            e.printStackTrace();
        }
    }
}