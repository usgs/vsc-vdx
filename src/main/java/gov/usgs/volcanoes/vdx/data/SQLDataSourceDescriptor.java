package gov.usgs.volcanoes.vdx.data;

import gov.usgs.util.ConfigFile;

/**
 * Keeps all information needed to construct particular data source
 *
 * @author Dan Cervelli, Loren Antolik
 */
public class SQLDataSourceDescriptor
{
	private String name;
	private String className;
	private String description;
	
	private ConfigFile params;
	
	private SQLDataSource sqlDataSource;
	
	/**
	 * Constructor
	 * @param n data source name
	 * @param c data source class name
	 * @param d data source description
	 * @param p configuration for data source
	 */
	public SQLDataSourceDescriptor(String n, String c, String d, ConfigFile p)
	{
		name		= n;
		className	= c;
		description	= d;
		params		= p;
	}
	
	/**
	 * Getter for data source name
	 * @return name
	 */
	public String getName()
	{
		return name;
	}
	
	/**
	 * Getter for data source class name
	 * return class name
	 */
	public String getClassName()
	{
		return className;
	}
	
	/**
	 * Getter for data source description
	 * @return description
	 */
	public String getDescription()
	{
		return description;
	}
	
	/**
	 * Getter for data source configuration
	 * @return params
	 */
	public ConfigFile getParams()
	{
		return params;
	}

	/**
	 * Instantiate data source
	 */
	private void instantiateDataSource()
	{
		try
		{
			sqlDataSource = (SQLDataSource)Class.forName(className).newInstance();
			Class.forName(className).getMethod("initialize", new Class[] { ConfigFile.class }).invoke(sqlDataSource, new Object[] { params });
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
	public SQLDataSource getSQLDataSource()
	{
		if (sqlDataSource == null && className != null)
			instantiateDataSource();
		
		return sqlDataSource;
	}
	
}
