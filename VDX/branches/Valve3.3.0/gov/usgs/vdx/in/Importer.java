package gov.usgs.vdx.in;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.ResourceReader;
import gov.usgs.vdx.data.Channel;
import gov.usgs.vdx.data.Column;
import gov.usgs.vdx.data.Rank;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.vdx.data.SQLDataSourceHandler;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
	
	protected ResourceReader rr;
	
	protected static Set<String> flags;
	protected static Set<String> keys;
	
	protected String vdxConfig;
	
	protected ConfigFile params;
	protected ConfigFile vdxParams;
	protected ConfigFile rankParams;
	protected ConfigFile columnParams;
	protected ConfigFile channelParams;
	protected ConfigFile dataSourceParams;
	protected ConfigFile translationParams;
	
	protected String driver, prefix, url;
	
	protected SimpleDateFormat dateIn;
	protected SimpleDateFormat dateOut;
	protected Date date;
	protected Double j2ksec;

	protected String filenameMask;
	protected int headerLines;
	protected String timestampMask;
	protected String timeZone;
	
	protected String importColumns;
	protected String[] importColumnArray;
	protected Map<Integer, String> importColumnMap;
	
	protected String dataSource;
	protected SQLDataSource sqlDataSource;
	protected SQLDataSourceHandler sqlDataSourceHandler;
	protected SQLDataSourceDescriptor sqlDataSourceDescriptor;	
	protected List<String> dataSourceList;
	protected Map<String, String> dataSourceColumnMap;
	protected Map<String, String> dataSourceChannelMap;
	protected Map<String, SQLDataSource> sqlDataSourceMap;
	protected Iterator<String> dsIterator;
	
	protected Rank rank;
	protected String rankName;
	protected int rankValue, rankDefault;
	
	protected String channels;
	protected String[] channelArray;
	protected Map<String, Channel> channelMap;	
	protected Channel channel;
	protected String channelCode, channelName;
	protected double channelLon, channelLat, channelHeight;
	protected List<String> channelList;
	protected Iterator<String> chIterator;
	protected String defaultChannels;
	
	protected String columns;
	protected String[] columnArray;
	protected HashMap<String, Column> columnMap;	
	protected Column column;
	protected String columnName, columnDescription, columnUnit;
	protected int columnIdx;
	protected boolean columnActive, columnChecked;
	protected List<String> columnList;
	protected Iterator<String> coIterator;
	protected String defaultColumns;
	
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
