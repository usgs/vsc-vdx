package gov.usgs.vdx.data;

import gov.usgs.util.ConfigFile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class DataSourceHandler
{
	private static final String CONFIG_FILE = "data.config";
	
	protected Logger logger;
	protected Map<String, DataSourceDescriptor> dataSources;
	private ConfigFile config;
	
	public DataSourceHandler(Logger log)
	{
		logger = log;
		dataSources = new HashMap<String, DataSourceDescriptor>();
		processConfigFile();
	}
	
	public void processConfigFile()
	{
		config = new ConfigFile(CONFIG_FILE);
		List sources = config.getList("source");
		for (Iterator it = sources.iterator(); it.hasNext(); )
		{
			String source = (String)it.next();
			logger.fine("read data source: " + source);
			ConfigFile sub = config.getSubConfig(source);
			DataSourceDescriptor dsd = new DataSourceDescriptor(source, sub.getString("class"), sub.getString("description"), sub.getConfig());
			dataSources.put(source, dsd);
		}
	}

	public ConfigFile getConfig()
	{
		return config;
	}
	
	public DataSourceDescriptor getDataSourceDescriptor(String key)
	{
		return dataSources.get(key);
	}
	
	public List<DataSourceDescriptor> getDataSources()
	{
		List<DataSourceDescriptor> result = new ArrayList<DataSourceDescriptor>();
		result.addAll(dataSources.values());
		return result;
	}

}
