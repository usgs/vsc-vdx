package gov.usgs.vdx.data;

import gov.usgs.util.ConfigFile;

/**
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
	
	public DataSourceDescriptor(String n, String c, String d, ConfigFile p)
	{
		name = n;
		dataClassName = c;
		description = d;
		
		params = p;
	}
	
	public String getDescription()
	{
		return description;
	}
	
	public String getName()
	{
		return name;
	}
	
	public String getClassName()
	{
		return dataClassName;
	}
	
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
	
	public DataSource getDataSource()
	{
		if (dataSource == null && dataClassName != null)
			instantiateDataSource();
		
		return dataSource;
	}
	
}
