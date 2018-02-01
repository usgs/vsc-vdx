package gov.usgs.volcanoes.vdx.data;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.vdx.ExportConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Construct list of data sources mentioned in the configuration file.
 *
 * @author Dan Cervelli
 * @author Loren Antolik
 * @author Bill Tollett
 */
public class DataSourceHandler {
  private static final String CONFIG_FILE = "vdxSources.config";
  private static final Logger LOGGER = LoggerFactory.getLogger(DataSourceHandler.class);
  protected Map<String, DataSourceDescriptor> dataSources;
  private ConfigFile config;
  private String driver;
  private String url;
  private String prefix;
  protected TreeMap<String, ExportConfig> exportConfigs;

  /**
   * Constructor.
   * 
   * @param d database driver class name
   * @param u database connection url
   * @param p vdx prefix
   */
  public DataSourceHandler(String d, String u, String p) {
    driver = d;
    url = u;
    prefix = p;

    dataSources = new HashMap<String, DataSourceDescriptor>();
    exportConfigs = new TreeMap<String, ExportConfig>();
    processConfigFile();
  }

  /**
   * Parse configuration file (default - 'vdxSources.config'). Fill map of pairs data_source_name -
   * initialized data_source_descriptor defined in the config file
   */
  public void processConfigFile() {
    config = new ConfigFile(CONFIG_FILE);
    List<String> sources = config.getList("source");
    for (Iterator<String> it = sources.iterator(); it.hasNext();) {
      String source = (String) it.next();
      ConfigFile sub = config.getSubConfig(source);
      
      sub.put("vdx.driver", driver);
      sub.put("vdx.url", url);
      sub.put("vdx.prefix", prefix);

      exportConfigs.put(source, new ExportConfig(source, sub));

      String className = sub.getString("class");
      String description = sub.getString("description");
      DataSourceDescriptor dsd = new DataSourceDescriptor(source, className, description, sub);
      dataSources.put(source, dsd);
      LOGGER.debug("read data source: {}", source);
    }
  }

  /**
   * Get parsed config file.
   * 
   * @return config file
   */
  public ConfigFile getConfig() {
    return config;
  }

  /**
   * Get data source descriptor by name.
   * 
   * @param key name of data source
   * @return data source descriptor
   */
  public DataSourceDescriptor getDataSourceDescriptor(String key) {
    return dataSources.get(key);
  }

  /**
   * Get the whole list of data sources.
   * 
   * @return List of data sources
   */
  public List<DataSourceDescriptor> getDataSources() {
    List<DataSourceDescriptor> result = new ArrayList<DataSourceDescriptor>();
    result.addAll(dataSources.values());
    return result;
  }

  /**
   * Getter for export config.
   * 
   * @param source name of data source
   * @return export config fo named source
   */
  public ExportConfig getExportConfig(String source) {
    return exportConfigs.get(source);
  }

  /**
   * Setter for export config.
   * 
   * @param source name of data source
   * @param ec export config
   */
  public void putExportConfig(String source, ExportConfig ec) {
    exportConfigs.put(source, ec);
  }
}
