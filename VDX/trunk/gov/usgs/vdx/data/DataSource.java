package gov.usgs.vdx.data;

import gov.usgs.util.ConfigFile;
import gov.usgs.vdx.server.RequestResult;

import java.util.Map;

/**
 * Interface to represent VDX data source
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
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
	 * Initialize this data source from configuration
	 */
	public void initialize(ConfigFile params);
	
	/**
	 * Retrieve data from data source
	 * @param params command as map of strings for 'parameter-value' pairs to specify query
	 * @return retrieved result of command execution
	 */
	public RequestResult getData(Map<String, String> params);
}
