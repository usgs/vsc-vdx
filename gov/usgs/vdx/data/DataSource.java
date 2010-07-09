package gov.usgs.vdx.data;

import gov.usgs.vdx.server.RequestResult;
import gov.usgs.util.ConfigFile;

import java.util.Map;

/**
 * Interface to represent VDX data source
 * 
 * @author Dan Cervelli
 */
public interface DataSource 
{
	/**
	 * Get data source type
	 */
	public String getType();
	
	/**
	 * Get max row count
	 */
	public int getMaxRows();
	
	/**
	 * Retrieve data from data source
	 * @param params command as map of strings for 'parameter-value' pairs to specify query
	 * @return retrieved result of command execution
	 */
	public RequestResult getData(Map<String, String> params);
	
	/**
	 * Initialize this data source from configuration
	 */
	public void initialize(ConfigFile params);
}
