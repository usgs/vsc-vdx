package gov.usgs.vdx.data;

import gov.usgs.util.ConfigFile;

/**
 * Keeps all information needed to construct particular data source
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class DataSourceDescriptor
{
	private String name;
	private String description;
	private String dataClassName;
	
	private ConfigFile params;
	
	private DataSource dataSource;
	
	/**
	 * Constructor
	 * @param n data source name
	 * @param c data source class name
	 * @param d data source description
	 * @param p configuration for data source
	 */
	public DataSourceDescriptor(String n, String c, String d, ConfigFile p)
	{
		name = n;
		dataClassName = c;
		description = d;
		
		params = p;
	}
	
	/**
	 * Getter for data source description
	 */
	public String getDescription()
	{
		return description;
	}
	
	/**
	 * Getter for data source name
	 * @return
	 */
	public String getName()
	{
		return name;
	}
	
	/**
	 * Getter for data source class name
	 */
	public String getClassName()
	{
		return dataClassName;
	}
	
	/**
	 * Getter for data source configuration
	 */
	public ConfigFile getParams()
	{
		return params;
	}

	private void instantiateDataSource()
	{
		try
		{
			dataSource = (DataSource)Class.forName(dataClassName).newInstance();
			Class.forName(dataClassName).getMethod("initialize", new Class[] { ConfigFile.class }).invoke(dataSource, new Object[] { params });
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	/**
	 * Construct data source using internal information if it isn't exist
	 * @return reference to constructed data source
	 */
	public DataSource getDataSource()
	{
		if (dataSource == null && dataClassName != null)
			instantiateDataSource();
		
		return dataSource;
	}
	
}
