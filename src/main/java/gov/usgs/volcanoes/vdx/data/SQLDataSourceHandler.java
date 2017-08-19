package gov.usgs.volcanoes.vdx.data;

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
 * @author Dan Cervelli, Loren Antolik, Bill Tollett
 */
public class SQLDataSourceHandler {
  private static final String CONFIG_FILE = "vdxSources.config";

  protected Logger logger;
  protected Map<String, SQLDataSourceDescriptor> sqlDataSources;
  private ConfigFile config;
  private String driver;
  private String url;
  private String prefix;

  /**
   * Constructor.
   * 
   * @param d database driver class name
   * @param u database connection url
   * @param p vdx prefix
   */
  public SQLDataSourceHandler(String d, String u, String p) {
    driver = d;
    url = u;
    prefix = p;

    logger = Log.getLogger("gov.usgs.volcanoes.vdx");
    sqlDataSources = new HashMap<String, SQLDataSourceDescriptor>();
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
      String className = sub.getString("class");
      String description = sub.getString("description");
      sub.put("vdx.driver", driver);
      sub.put("vdx.url", url);
      sub.put("vdx.prefix", prefix);
      SQLDataSourceDescriptor dsd =
          new SQLDataSourceDescriptor(source, className, description, sub);
      sqlDataSources.put(source, dsd);
      logger.fine("read data source: " + source);
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
   * @param key datasource name
   * @return source descriptor
   */
  public SQLDataSourceDescriptor getDataSourceDescriptor(String key) {
    return sqlDataSources.get(key);
  }

  /**
   * Get the whole list of data sources.
   * 
   * @return list of source descriptors
   */
  public List<SQLDataSourceDescriptor> getDataSources() {
    List<SQLDataSourceDescriptor> result = new ArrayList<SQLDataSourceDescriptor>();
    result.addAll(sqlDataSources.values());
    return result;
  }
}
