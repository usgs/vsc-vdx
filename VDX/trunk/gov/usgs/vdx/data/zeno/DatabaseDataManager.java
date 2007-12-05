package gov.usgs.vdx.data.zeno;

import java.sql.*;

/**
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
    
    protected DatabaseDataManager() {}
    
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