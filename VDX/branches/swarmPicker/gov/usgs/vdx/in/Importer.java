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
	 * @param importerClass name of importer class
	 * @param configFile configuration file
	 * @param verbose true for info, false for severe
	 */
	public void initialize(String importerClass, String configFile, boolean verbose);
	
	/**
	 * Deinitialize importer.  Concrete realization see in the inherited classes
	 */
	public void deinitialize();
	
	/**
	 * Process config file.  Reads a config file and parses contents into local variables
	 * @param configFile configuration file
	 */
	public void processConfigFile(String configFile);
	
	/**
	 * Process.  Reads a file and parses the contents to the database
	 * @param filename file to import
	 */
	public void process(String filename);
	
	/**
	 * Print usage.  Prints out usage instructions for the given importer
	 * @param importerClass name of importer class
	 * @param message instructions
	 */
	public void outputInstructions(String importerClass, String message);
}
