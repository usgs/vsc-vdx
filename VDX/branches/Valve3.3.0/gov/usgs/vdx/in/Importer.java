package gov.usgs.vdx.in;

/**
 * Importer
 * Abstract class to import data
 * into database.
 * 
 * @author Loren Antolik
 */
public interface Importer {
	
	/**
	 * Initialize importer.  Concrete realization see in the inherited classes
	 */
	public void initialize(String importerClass, String configFile, boolean verbose);
	
	/**
	 * Process config file.  Reads a config file and parses contents into local variables
	 */
	public void processConfigFile(String configFile);
	
	/**
	 * Print usage.  Prints out usage instructions for the given importer
	 */
	public void outputInstructions(String importerClass, String message);
}
