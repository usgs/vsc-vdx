package gov.usgs.vdx.data;

import gov.usgs.vdx.server.RequestResult;
// import gov.usgs.util.ConfigFile;
// import gov.usgs.vdx.db.VDXDatabase;

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
	 * Retrieve data from data source
	 * @param params command as map of strings for 'parameter-value' pairs to specify query
	 * @return retrieved result of command execution
	 */
	public RequestResult getData(Map<String, String> params);
	
	/**
	 * Initialize this data source from configuration
	 */
	// LJA removed these references until a common standard could be acheived for VALVE and Winston
	// public void initialize(ConfigFile params);  (Winston)
	// public void initialize(VDXDatabase db, String name, String type); (VALVE)
}
