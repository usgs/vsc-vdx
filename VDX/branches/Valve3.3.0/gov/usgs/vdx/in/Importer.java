package gov.usgs.vdx.in;

import gov.usgs.util.ConfigFile;
import gov.usgs.vdx.data.Rank;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.vdx.data.SQLDataSourceHandler;

import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Importer
 * Abstract class to import data
 * into database.
 * 
 * @author Loren Antolik
 */
abstract public class Importer {
	
	protected static Set<String> flags;
	protected static Set<String> keys;
	
	protected String vdxConfig;
	
	protected ConfigFile params;
	protected ConfigFile vdxParams;
	
	protected String driver, prefix, url;
	
	protected String dataSource;
	protected SQLDataSource sqlDataSource;
	protected SQLDataSourceHandler sqlDataSourceHandler;
	protected SQLDataSourceDescriptor sqlDataSourceDescriptor;
	
	protected SimpleDateFormat dateIn;

	protected int headerLines;
	protected String timestampMask;
	protected String timeZone;
	
	protected Rank rank;
	protected String rankName;
	protected int rankValue, rankDefault;
	protected ConfigFile rankParams;
	
	protected String importerType;
	
	protected Logger logger;
	
	static {
		flags	= new HashSet<String>();
		keys	= new HashSet<String>();
		keys.add("-c");
		flags.add("-h");
		flags.add("-v");
	}
	
	/**
	 * Initialize importer.  Concrete realization see in the inherited classes
	 */
	abstract public void initialize(String importerClass, String configFile, boolean verbose);
	
	/**
	 * Process config file.  Reads a config file and parses contents into local variables
	 */
	abstract public void processConfigFile(String configFile);
	
	/**
	 * Process file.  Reads a resource and parses it's contents into the database
	 */
	abstract public void process(String filename);
	
	/**
	 * Print usage.  Prints out usage instructions for the given importer
	 */
	abstract public void outputInstructions(String importerClass, String message);
	
	/**
	 * Initialize Importer
	 */
	public void defaultInitialize(String importerClass, boolean verbose) {
		
		// initialize the logger for this importer
		logger	= Logger.getLogger(importerClass);
		logger.log(Level.INFO, "Importer.defaultInitialize() succeeded.");
	}
	
	/**
	 * Output Instructions for usage
	 */
	public void defaultOutputInstructions(String importerClass, String message) {
		if (message == null) {
			System.err.println(message);
		}
		System.err.println(importerClass + " -c configfile filelist");
	}
}
