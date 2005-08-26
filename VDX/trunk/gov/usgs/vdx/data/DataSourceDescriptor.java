package gov.usgs.vdx.data;

import java.util.Map;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class DataSourceDescriptor
{
	private String name;
	private String description;
	private String dataClassName;
	
	private Map<String, Object> params;
	
	private DataSource dataSource;
	
	public DataSourceDescriptor(String n, String c, String d, Map<String, Object> p)
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
	
	public Map<String, Object> getParams()
	{
		return params;
	}

	private void instantiateDataSource()
	{
		try
		{
			dataSource = (DataSource)Class.forName(dataClassName).newInstance();
			Class.forName(dataClassName).getMethod("initialize", new Class[] { Map.class }).invoke(dataSource, new Object[] { params });
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
