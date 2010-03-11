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
	 * Deinitialize importer.  Concrete realization see in the inherited classes
	 */
	public void deinitialize();
	
	/**
	 * Process config file.  Reads a config file and parses contents into local variables
	 */
	public void processConfigFile(String configFile);
	
	/**
	 * Process.  Reads a file and parses the contents to the database
	 */
	public void process(String filename);
	
	/**
	 * Print usage.  Prints out usage instructions for the given importer
	 */
	public void outputInstructions(String importerClass, String message);
}
