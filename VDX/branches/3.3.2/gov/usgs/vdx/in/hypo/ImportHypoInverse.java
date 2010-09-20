package gov.usgs.vdx.in.hypo;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.Channel;
import gov.usgs.vdx.data.Column;
import gov.usgs.vdx.data.Rank;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.vdx.data.SQLDataSourceHandler;
import gov.usgs.vdx.data.hypo.Hypocenter;
import gov.usgs.vdx.data.hypo.SQLHypocenterDataSource;
import gov.usgs.vdx.in.Importer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Import HypoInverse files
 *  
 * @author Loren Antolik
 */
public class ImportHypoInverse implements Importer {
	
	public ResourceReader rr;
	
	public static Set<String> flags;
	public static Set<String> keys;
	
	public String vdxConfig;
	
	public ConfigFile params;
	public ConfigFile vdxParams;
	public ConfigFile rankParams;
	public ConfigFile columnParams;
	public ConfigFile channelParams;
	public ConfigFile dataSourceParams;
	public ConfigFile translationParams;
	
	public String driver, prefix, url;
	
	public SimpleDateFormat dateIn;
	public SimpleDateFormat dateOut;
	public Date date;
	public Double j2ksec;

	public String filenameMask;
	public int headerLines;
	public String timestampMask;
	public String timeZone;
	
	public String importColumns;
	public String[] importColumnArray;
	public Map<Integer, String> importColumnMap;
	
	public String dataSource;
	public SQLHypocenterDataSource sqlDataSource;
	public SQLDataSourceHandler sqlDataSourceHandler;
	public SQLDataSourceDescriptor sqlDataSourceDescriptor;	
	public List<String> dataSourceList;
	public Iterator<String> dsIterator;
	public Map<String, SQLDataSource> sqlDataSourceMap;
	public Map<String, String> dataSourceColumnMap;
	public Map<String, String> dataSourceChannelMap;
	public Map<String, Integer>	dataSourceRankMap;
	
	public Rank rank;
	public String rankName;
	public int rankValue, rankDefault;
	public int rid;
	
	public String channels;
	public String[] channelArray;
	public Map<String, Channel> channelMap;	
	public Channel channel;
	public String channelCode, channelName;
	public double channelLon, channelLat, channelHeight;
	public List<String> channelList;
	public Iterator<String> chIterator;
	public String defaultChannels;
	
	public String columns;
	public String[] columnArray;
	public HashMap<String, Column> columnMap;	
	public Column column;
	public String columnName, columnDescription, columnUnit;
	public int columnIdx;
	public boolean columnActive, columnChecked;
	public List<String> columnList;
	public Iterator<String> coIterator;
	public String defaultColumns;
	
	public String importerType = "hypocenters";
	
	public Logger logger;
	
	static {
		flags	= new HashSet<String>();
		keys	= new HashSet<String>();
		keys.add("-c");
		flags.add("-h");
		flags.add("-v");
	}

	/**
	 * takes a config file as a parameter and parses it to prepare for importing
	 * @param cf configuration file
	 * @param verbose true for info, false for severe
	 */
	public void initialize(String importerClass, String configFile, boolean verbose) {
		
		// initialize the logger for this importer
		logger	= Logger.getLogger(importerClass);
		logger.log(Level.INFO, "ImportHypoInverse.initialize() succeeded.");
		
		// process the config file
		processConfigFile(configFile);
	}
	
	/**
	 * disconnects from the database
	 */
	public void deinitialize() {
		sqlDataSource.disconnect();
	}
	
	/**
	 * Parse configuration file.  This sets class variables used in the importing process
	 * @param configFile	name of the config file
	 */
	public void processConfigFile(String configFile) {
		
		logger.log(Level.INFO, "Reading config file " + configFile);
		
		// initialize the config file and verify that it was read
		params		= new ConfigFile(configFile);
		if (!params.wasSuccessfullyRead()) {
			logger.log(Level.SEVERE, configFile + " was not successfully read");
			System.exit(-1);
		}
		
		// get the vdx parameter, and exit if it's missing
		vdxConfig	= Util.stringToString(params.getString("vdx.config"), "VDX.config");
		if (vdxConfig == null) {
			logger.log(Level.SEVERE, "vdx.config parameter missing from config file");
			System.exit(-1);
		}
		
		// get the vdx config as it's own config file object
		vdxParams	= new ConfigFile(vdxConfig);
		driver		= vdxParams.getString("vdx.driver");
		url			= vdxParams.getString("vdx.url");
		prefix		= vdxParams.getString("vdx.prefix");
		
		// define the data source handler that acts as a wrapper for data sources
		sqlDataSourceHandler	= new SQLDataSourceHandler(driver, url, prefix);
		
		// get the list of data sources that are being used in this import
		dataSource	= params.getString("dataSource");
				
		// lookup the data source from the list that is in vdxSources.config
		sqlDataSourceDescriptor	= sqlDataSourceHandler.getDataSourceDescriptor(dataSource);
		if (sqlDataSourceDescriptor == null) {
			logger.log(Level.SEVERE, dataSource + " sql data source does not exist in vdxSources.config");
		}
				
		// formally get the data source from the list of descriptors.  this will initialize the data source which includes db creation
		sqlDataSource	= (SQLHypocenterDataSource)sqlDataSourceDescriptor.getSQLDataSource();
		
		if (!sqlDataSource.getType().equals(importerType)) {
			logger.log(Level.SEVERE, "dataSource not a " + importerType + " data source");
			System.exit(-1);
		}
		
		// information related to the timestamps
		timestampMask	= "yyyyMMddHHmmssSS";
		timeZone		= "GMT";
		dateIn			= new SimpleDateFormat(timestampMask);
		dateIn.setTimeZone(TimeZone.getTimeZone(timeZone));
		
		// get the list of ranks that are being used in this import
		rankParams		= params.getSubConfig("rank");
		rankName		= Util.stringToString(rankParams.getString("name"), "DEFAULT");
		rankValue		= Util.stringToInt(rankParams.getString("value"), 1);
		rankDefault		= Util.stringToInt(rankParams.getString("default"), 0);
		rank			= new Rank(0, rankName, rankValue, rankDefault);
		
		// create rank entry
		if (sqlDataSource.getRanksFlag()) {
			Rank tempRank	= sqlDataSource.defaultGetRank(rank);
			if (tempRank == null) {
				tempRank = sqlDataSource.defaultInsertRank(rank);
			}
			if (tempRank == null) {
				logger.log(Level.SEVERE, "invalid rank for dataSource " + dataSource);
				System.exit(-1);
			}
			rid	= tempRank.getId();
		}
	}
	
	/**
	 * Parse hypoinverse file from url (resource locator or file name)
	 * @param filename
	 */
	public void process(String filename) {
		
		// initialize variables local to this method
		double j2ksec, lat, lon, depth, prefmag;
		String eid;
		Double ampmag	= Double.NaN;
		Double codamag	= Double.NaN;
		Double dmin		= Double.NaN;
		Double rms		= Double.NaN;
		Double herr		= Double.NaN;
		Double verr		= Double.NaN;
		String magtype	= null;
		String rmk		= null;
		Integer nphases	= null;
		Integer azgap	= null;
		Integer nstimes	= null;
		
		try {
			
			// check that the file exists
			rr = ResourceReader.getResourceReader(filename);
			if (rr == null) {
				logger.log(Level.SEVERE, "skipping: " + filename + " (resource is invalid)");
				return;
			}
			
			// move to the first line in the file
			String line		= rr.nextLine();
			int lineNumber	= 1;
			
			// check that the file has data
			if (line == null) {
				logger.log(Level.SEVERE, "skipping: " + filename + " (resource is empty)");
				return;
			}
			
			logger.log(Level.INFO, "importing: " + filename);

			while (line != null) {
				
				logger.log(Level.INFO, "importing: line number " + lineNumber);
				
				// DATE
				try {
					String timestamp	= line.substring(0,16) + "0";
					date				= dateIn.parse(timestamp);
					j2ksec				= Util.dateToJ2K(date);
				} catch (ParseException e) {
					logger.log(Level.SEVERE, "skipping: line number " + lineNumber + ".  Timestamp not valid.");					
					line	= rr.nextLine();
					lineNumber++;
					continue;
				}
				
				// EID
				eid			= line.substring(136, 146).trim();
				if (eid.trim().length() == 0) {
					logger.log(Level.SEVERE, "skipping: line number " + lineNumber + ".  Event ID not valid.");					
					line	= rr.nextLine();
					lineNumber++;
					continue;
				}

				// LAT
				double latdeg	= Double.parseDouble(line.substring(16, 18).trim());
				double latmin	= Double.parseDouble(line.substring(19, 21).trim() + "." + line.substring(21, 23).trim());
				lat				= latdeg + ( latmin / 60.0d );
				char ns			= line.charAt(18);
				if (ns == 'S')
					lat *= -1;

				// LON
				double londeg	= Double.parseDouble(line.substring(23, 26).trim());
				double lonmin	= Double.parseDouble(line.substring(27, 29).trim() + "." + line.substring(29, 31).trim());
				lon				= londeg + ( lonmin / 60.0d );
				char ew			= line.charAt(26);
				if (ew != 'E')
					lon *= -1;
				
				// DEPTH
				try {
					depth		= Double.parseDouble(line.substring(31, 34).trim() + "." + line.substring(34, 36).trim());
					depth *= -1;
				} catch (NumberFormatException e) {
					logger.log(Level.SEVERE, "skipping: line number " + lineNumber + ".  Depth not valid.");					
					line	= rr.nextLine();
					lineNumber++;
					continue;
				}
				
				// PREFERRED MAGNITUDE
				try {
					prefmag 	= Double.parseDouble(line.substring(147, 150).trim()) / 100;
				} catch (NumberFormatException e) {
					prefmag		= Double.NaN;
				}				
				
				// AMPLITUDE MAGNITUDE
				try {				
					ampmag 		= Double.parseDouble(line.substring(36, 39).trim()) / 100;
				} catch (NumberFormatException e) {
					ampmag		= Double.NaN;
				}
				
				// CODA MAGNITUDE
				try {
					codamag 	= Double.parseDouble(line.substring(70, 73).trim()) / 100;
				} catch (NumberFormatException e) {
					codamag		= Double.NaN;
				}
				
				// NPHASES
				try {
					nphases		= Integer.parseInt(line.substring(39, 42).trim());
				} catch (NumberFormatException e) {
					nphases		= 0;
				}
				
				// AZGAP
				try {
					azgap		= Integer.parseInt(line.substring(42, 45).trim());
				} catch (NumberFormatException e) {
					azgap		= null;
				}
				
				// DMIN
				try {
					dmin		= Double.parseDouble(line.substring(45, 48).trim());
				} catch (NumberFormatException e) {
					dmin		= Double.NaN;
				}
				
				// RMS
				try {
					rms			= Double.parseDouble(line.substring(48, 52).trim()) / 100;
				} catch (NumberFormatException e) {
					rms			= Double.NaN;
				}
				
				// NSTIMES
				try {
					nstimes		= Integer.parseInt(line.substring(82, 85).trim());
				} catch (NumberFormatException e) {
					nstimes		= 0;
				}
				
				// HERR
				try {
					herr		= Double.parseDouble(line.substring(85, 89).trim()) / 100;
				} catch (NumberFormatException e) {
					herr		= Double.NaN;
				}
				
				// VERR
				try {
					verr		= Double.parseDouble(line.substring(89, 93).trim()) / 100;
				} catch (NumberFormatException e) {
					verr		= Double.NaN;
				}
				
				// RMK
				rmk			= line.substring(80, 81).trim();
				if (rmk.trim().length() == 0) {
					rmk		= null;
				}
				
				// MAGTYPE
				magtype		= line.substring(146, 147).trim();
				if (magtype.trim().length() == 0) {
					magtype		= null;
				}
				
				Hypocenter hc	= new Hypocenter(j2ksec, eid, rid, lat, lon, depth, prefmag, ampmag, codamag, 
						nphases, azgap, dmin, rms, nstimes, herr, verr, magtype, rmk);
				sqlDataSource.insertHypocenter(hc);
				
				line	= rr.nextLine();
				lineNumber++;
			}
				
		} catch (Exception e) {
			logger.log(Level.SEVERE, "ImportHypoInverse.process(" + filename + ") failed.", e);
		}
	}
	
	public void outputInstructions(String importerClass, String message) {
		if (message == null) {
			System.err.println(message);
		}
		System.err.println(importerClass + " -c configfile filelist");
	}

	/**
	 * Main method.
	 * Command line syntax:
	 *  -h, --help print help message
	 *  -c config file name
	 *  -v verbose mode
	 *  files ...
	 */
	public static void main(String as[]) {
		
		ImportHypoInverse importer	= new ImportHypoInverse();
		
		Arguments args = new Arguments(as, flags, keys);
		
		if (args.flagged("-h")) {
			importer.outputInstructions(importer.getClass().getName(), null);
			System.exit(-1);
		}
		
		if (!args.contains("-c")) {
			importer.outputInstructions(importer.getClass().getName(), "Config file required");
			System.exit(-1);
		}

		importer.initialize(importer.getClass().getName(), args.get("-c"), args.flagged("-v"));

		List<String> files	= args.unused();
		for (String file : files) {
			importer.process(file);
		}
		
		importer.deinitialize();
	}	
}