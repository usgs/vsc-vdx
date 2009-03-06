package gov.usgs.vdx.data;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Construct list of data sources mentioned in the configuration file.
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.2  2005/08/29 15:55:54  dcervelli
 * New logging changes.
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class DataSourceHandler
{
	private static final String CONFIG_FILE = "vdxSources.config";
	
	protected Logger logger;
	protected Map<String, DataSourceDescriptor> dataSources;
	private ConfigFile config;
	private String driver;
	private String url;
	private String vdxPrefix;
	
	/**
	 * Constructor
	 * @param d database driver class name
	 * @param u database connection url
	 * @param p vdx prefix
	 */
	public DataSourceHandler(String d, String u, String p)
	{
		driver = d;
		url = u;
		vdxPrefix = p;
		
		logger = Log.getLogger("gov.usgs.vdx");
		dataSources = new HashMap<String, DataSourceDescriptor>();
		processConfigFile();
	}

	/**
	 * Parse configuration file (default - 'vdxSources.config').
	 * Fill map of pairs data_source_name - initialized data_source_descriptor defined in the config file
	 */
	public void processConfigFile()
	{
		config = new ConfigFile(CONFIG_FILE);
		List sources = config.getList("source");
		for (Iterator it = sources.iterator(); it.hasNext(); )
		{
			String source = (String)it.next();
			logger.fine("read data source: " + source);
			ConfigFile sub = config.getSubConfig(source);
			sub.put("vdx.driver", driver);
			sub.put("vdx.url", url);
			sub.put("vdx.vdxPrefix", vdxPrefix);
			DataSourceDescriptor dsd = new DataSourceDescriptor(source, sub.getString("class"), sub.getString("description"), sub);
			dataSources.put(source, dsd);
		}
	}

	/**
	 * Get parsed config file
	 */
	public ConfigFile getConfig()
	{
		return config;
	}
	
	/**
	 * Get data source descriptor by name
	 */
	public DataSourceDescriptor getDataSourceDescriptor(String key)
	{
		return dataSources.get(key);
	}
	
	/**
	 * Get the whole list of data sources
	 */
	public List<DataSourceDescriptor> getDataSources()
	{
		List<DataSourceDescriptor> result = new ArrayList<DataSourceDescriptor>();
		result.addAll(dataSources.values());
		return result;
	}
}
