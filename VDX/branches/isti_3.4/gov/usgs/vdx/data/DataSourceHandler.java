package gov.usgs.vdx.data;

import gov.usgs.util.ConfigFile;
import gov.usgs.vdx.ExportConfig;
import gov.usgs.util.Log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * Construct list of data sources mentioned in the configuration file.
 *
 * @author Dan Cervelli, Loren Antolik
 */
public class DataSourceHandler
{
	private static final String CONFIG_FILE = "vdxSources.config";
	
	protected Logger logger;
	protected Map<String, DataSourceDescriptor> dataSources;
	private ConfigFile config;
	private String driver;
	private String url;
	private String prefix;
	protected TreeMap<String,ExportConfig> exportConfigs;
	
	/**
	 * Constructor
	 * @param d database driver class name
	 * @param u database connection url
	 * @param p vdx prefix
	 */
	public DataSourceHandler(String d, String u, String p) {
		driver	= d;
		url		= u;
		prefix	= p;
		
		logger		= Log.getLogger("gov.usgs.vdx");
		dataSources	= new HashMap<String, DataSourceDescriptor>();
		exportConfigs = new TreeMap<String,ExportConfig>();
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
		for (Iterator it = sources.iterator(); it.hasNext(); ) {
			String source		= (String)it.next();
			ConfigFile sub		= config.getSubConfig(source);
			String className	= sub.getString("class");
			String description	= sub.getString("description");
			sub.put("vdx.driver", driver);
			sub.put("vdx.url", url);
			sub.put("vdx.prefix", prefix);
			
			exportConfigs.put( source, new ExportConfig( source, sub ) );

			DataSourceDescriptor dsd = new DataSourceDescriptor(source, className, description, sub);
			dataSources.put(source, dsd);
			logger.fine("read data source: " + source);
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

	/**
	 * Getter for export config
	 */
	public ExportConfig getExportConfig( String source ) 
	{
		return exportConfigs.get(source);
	}

	/**
	 * Setter for export config
	 */
	public void putExportConfig( String source, ExportConfig ec ) 
	{
		exportConfigs.put(source, ec);
	}
}
