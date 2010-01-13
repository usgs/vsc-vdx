package gov.usgs.vdx.in;

import java.text.SimpleDateFormat;
import java.text.ParseException;
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

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.Channel;
import gov.usgs.vdx.data.Column;
import gov.usgs.vdx.data.SQLDataSourceHandler;
import gov.usgs.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.vdx.data.GenericDataMatrix;
import gov.usgs.vdx.data.Rank;
import gov.usgs.vdx.data.SQLDataSource;

import cern.colt.matrix.*;

/**
 * Class for importing files.
 *  
 * @author Loren Antolik
 */
public class ImportFile {
	
	private Logger logger;
	
	private ConfigFile params;
	private ConfigFile vdxParams;
	private ConfigFile columnParams;
	private ConfigFile channelParams;
	private ConfigFile rankParams;
	private ConfigFile dataSourceParams;
	
	private String driver, prefix, url;

	private String filenameMask;
	private int headerLines;
	private String timestampMask;
	private String timezone;
	
	private SimpleDateFormat dateIn;
	private SimpleDateFormat dateOut;
	private Date date;
	private double j2ksec;
	
	private String columns;
	private String[] columnArray;
	private HashMap<Integer, String> columnMap;	
	
	private Channel channel;
	private String channelCode, channelName;
	private double channelLon, channelLat, channelHeight;
	private List<String> channelList;
	private Map<String, Channel> channelMap;
	
	private Rank rank;
	private String rankCode, rankName;
	private int rankValue, rankDefault;
	private List<String> rankList;
	private Map<String, Rank> rankMap;
	private int rid;
	
	private String dataSource;
	private List<String> dataSourceList;
	private Map<String, String> dataSourceColumnMap;
	private SQLDataSource sqlDataSource;	
	private Map<String, SQLDataSource> sqlDataSourceMap;
	private SQLDataSourceHandler sqlDataSourceHandler;
	private SQLDataSourceDescriptor sqlDataSourceDescriptor;
	
	private GenericDataMatrix gdm;
	int count;
	int test;

	/**
	 * takes a config file as a parameter and parses it to prepare for importing
	 * @param cf configuration file
	 */
	public void initialize(String configFile, boolean verbose) {
		
		// initialize the logger		
		logger = Logger.getLogger("gov.usgs.vdx.in.ImportFile");		
		if (verbose) {
			logger.setLevel(Level.ALL);
		} else {
			logger.setLevel(Level.INFO);
		}
		
		// process the config file
		processConfigFile(configFile);
	}
	
	/**
	 * Parse configuration file.  This sets class variables used in the importing process
	 * @param configFile	name of the config file
	 */
	private void processConfigFile(String configFile) {
		
		logger.log(Level.INFO, "Reading config file " + configFile);
		
		// instantiate the config file
		params		= new ConfigFile(configFile);
		
		// get the vdx parameter, and exit if it's missing
		String vdxConfig	= params.getString("vdx.config");
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
		sqlDataSourceMap		= new HashMap<String, SQLDataSource>();
		
		// information related to a row of data in this import.  be sure to keep these in order!
		columns			= params.getString("datarowMask");
		columnArray		= columns.split(",");
		columnMap		= new HashMap<Integer, String>();
		for (int i = 0; i < columnArray.length; i++) {
			columnMap.put(i, columnArray[i].trim());
		}
		
		// information related to information in the filename for this file
		filenameMask	= Util.stringToString(params.getString("filenameMask"), "");
		
		// information related to header lines in this file
		headerLines	= Util.stringToInt(params.getString("headerlines"), 0);
		
		// information related to the timestamps
		timestampMask	= Util.stringToString(params.getString("timestampMask"), "yyyy-MM-dd HH:mm:ss");
		timezone		= Util.stringToString(params.getString("timezone"), "GMT");
		dateIn			= new SimpleDateFormat(timestampMask);
		dateOut			= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		dateIn.setTimeZone(TimeZone.getTimeZone(timezone));
		dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));
		
		// get the list of channels that are being used in this import
		channelList					= params.getList("channel");
		if (channelList != null) {
			Iterator<String> chIterator	= channelList.iterator();
			channelMap					= new HashMap<String, Channel>();
			while (chIterator.hasNext()) {
				channelCode		= chIterator.next();
				channelParams	= params.getSubConfig(channelCode);
				channelName		= Util.stringToString(channelParams.getString("name"), channelCode);
				channelLon		= Util.stringToDouble(channelParams.getString("lon"), Double.NaN);
				channelLat		= Util.stringToDouble(channelParams.getString("lat"), Double.NaN);
				channelHeight	= Util.stringToDouble(channelParams.getString("height"), Double.NaN);
				channel			= new Channel(0, channelCode, channelName, channelLon, channelLat, channelHeight);
				channelMap.put(channelCode, channel);
			}
		}
		
		// get the list of ranks that are being used in this import
		rankList					= params.getList("rank");
		if (rankList != null) {
			Iterator<String> rkIterator	= rankList.iterator();
			rankMap						= new HashMap<String, Rank>();
			while (rkIterator.hasNext()) {
				rankCode		= params.getString("rank");
				rankParams		= params.getSubConfig(rankCode);
				rankName		= Util.stringToString(rankParams.getString("name"), rankCode);
				rankValue		= Util.stringToInt(rankParams.getString("value"), 1);
				rankDefault		= Util.stringToInt(rankParams.getString("default"), 1);
				rank			= new Rank(0, rankName, rankValue, rankDefault);
				rankMap.put(rankCode, rank);
			}
		}
		
		// get the list of data sources that are being used in this import
		dataSourceList				= params.getList("dataSource");
		if (dataSourceList != null) {
			Iterator<String> dsIterator	= dataSourceList.iterator();
			dataSourceColumnMap			= new HashMap<String, String>();		
			while (dsIterator.hasNext()) {
				
				// get the data source name and define the columns that it contains
				dataSource			= dsIterator.next();
				dataSourceParams	= params.getSubConfig(dataSource);
				columns				= dataSourceParams.getString("columns");
				dataSourceColumnMap.put(dataSource, columns);
				
				// lookup the data source from the list that is in vdxSources.config
				sqlDataSourceDescriptor	= sqlDataSourceHandler.getDataSourceDescriptor(dataSource);
				if (sqlDataSourceDescriptor == null) {
					logger.log(Level.SEVERE, dataSource + " sql data source does not exist in vdxSources.config");
					continue;
				}
				
				// formally get the data source from the list of descriptors.  this will initialize the data source which includes db creation
				sqlDataSource			= sqlDataSourceDescriptor.getSQLDataSource();
				
				// store the reference to the initialized data source in the map of initialized data sources
				sqlDataSourceMap.put(dataSource, sqlDataSource);
					
				// look up column information for this data source and save to the database
				columnArray	= columns.split(",");
				for (int i = 0; i < columnArray.length; i++) {
					columnParams		= dataSourceParams.getSubConfig(columnArray[i]);
					String name			= columnArray[i];
					int idx				= Util.stringToInt(columnParams.getString("idx"));
					String description	= columnParams.getString("description");
					String unit			= columnParams.getString("unit");
					boolean active		= Util.stringToBoolean(columnParams.getString("active"), true);
					boolean checked		= Util.stringToBoolean(columnParams.getString("checked"), true);
					Column column 		= new Column(idx, name, description, unit, checked, active);
					if (sqlDataSource.defaultGetColumn(name) == null)	{
						sqlDataSource.defaultInsertColumn(column);
					}
				}
				
				// if translations are enabled then create the translation table
				if (sqlDataSource.getTranslationsFlag()) {
					sqlDataSource.defaultCreateTranslation();
				}
				
				// create rank entry
				if (sqlDataSource.getRanksFlag()) {
					sqlDataSource.defaultInsertRank(rank);
				}
	
				// create channels tables
				if (channelList != null) {
					for (int i = 0; i < channelList.size(); i++) {
						channelCode	= channelList.get(i);
						channel		= channelMap.get(channelCode);
						sqlDataSource.defaultCreateChannel(channel, 
								sqlDataSource.getChannelsFlag(), sqlDataSource.getTranslationsFlag(), 
								sqlDataSource.getRanksFlag(), sqlDataSource.getColumnsFlag());
					}
				}
			}
		}
	}
	
	/**
	 * Import File into database based on configuration parameters
	 * @param filename name of file to process
	 */
	public void process(String filename) {
		
		try {
			
			// check that the file exists
			ResourceReader rr = ResourceReader.getResourceReader(filename);
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
			
			logger.info("importing: " + filename);
			
			// if a filename mask is defined, then get the channel code from it
			if (filenameMask.length() > 0) {
				channelCode				= "";
				String shortFileName	= filename.substring(filename.lastIndexOf("/") + 1);
				
				// filename mask can be shorter than the filename, but not longer
				if (filenameMask.length() > shortFileName.length()) {
					logger.log(Level.SEVERE, "skipping: " + filename + " (bad filename mask)");
					return;
				}
				
				// build up the channel code from the mask
				for (int i = 0; i < filenameMask.length(); i++) {
					if (String.valueOf(filenameMask.charAt(i)).equals("C")) {
						channelCode	= channelCode + String.valueOf(shortFileName.charAt(i));
					}
				}
			}
			
			// if any header lines are defined then skip them
			if (headerLines > 0) {
				logger.log(Level.INFO, "skipping " + headerLines + " header lines");
				for (int i = 0; i < headerLines; i++) {
					line	= rr.nextLine();
					lineNumber++;
				}
			}
			
			// we are now at the first row of data.  time to import!
			while (line != null) {
				
				// split the data row into an ordered list.  
				// be sure to use the two argument split, as some lines may have many trailing delimiters
				String[] valueArray					= line.split(",", -1);
				HashMap<Integer, String> valueMap	= new HashMap<Integer, String>();
				for (int i = 0; i < valueArray.length; i++) {
					valueMap.put(i, valueArray[i].trim());
				}
				
				// make sure the data row matches the defined data columns
				if (columnMap.size() != valueMap.size()) {
					if (columnMap.size() > valueMap.size()) {
						logger.log(Level.SEVERE, "Skipping line " + lineNumber + " (too few values)");
					} else {
						logger.log(Level.SEVERE, "Skipping line " + lineNumber + " (too many values)");
					}
					line	= rr.nextLine();
					lineNumber++;
					continue;
				}
				
				// map the columns to the values.  look for the TIMESTAMP and CHANNEL flags
				HashMap<Integer, ColumnValue> columnValueMap = new HashMap<Integer, ColumnValue>();
				ColumnValue columnValue;
				String name;
				double value;
				count			= 0;
				String tsValue	= "";
				for (int i = 0; i < columnMap.size(); i++) {					
					name		= columnMap.get(i);
					
					// skip IGNORE columns
					if (name.equals("IGNORE")) {
						continue;

					// parse out the CHANNEL
					} else if (name.equals("CHANNEL")) {
						channelCode	= valueMap.get(i);
						continue;
						
					// parse out the TIMESTAMP
					} else if (name.equals("TIMESTAMP")) {
						tsValue	= tsValue + valueMap.get(i) + " ";
						continue;
						
					// elements that are neither IGNORE nor CHANNELS nor TIMESTAMPS are DATA	
					} else {					
						if (valueMap.get(i).length() == 0) {
							value	= Double.NaN;
						} else {
							value	= Double.parseDouble(valueMap.get(i));
						}
						columnValue	= new ColumnValue(name, value);
						columnValueMap.put(count, columnValue);
						count++;
					}
				}
				
				// make sure that the channel code has something in it
				if (channelCode.length() == 0) {
					logger.log(Level.SEVERE, "Skipping line " + lineNumber + " (channel code not found)");
					line	= rr.nextLine();
					lineNumber++;
					continue;
					
				// convert bad sql characters to dollar signs
				} else {
					channelCode = channelCode.replace('\\', '$').replace('/', '$').replace('.', '$');
				}
				
				// make sure that the timestamp has something in it
				if (tsValue.length() == 0) {
					logger.log(Level.SEVERE, "Skipping line " + lineNumber + " (timestamp not found)");
					line	= rr.nextLine();
					lineNumber++;
					continue;
				}
				
				// convert the timezone of the input date and convert to j2ksec
				try {
					date	= dateIn.parse(tsValue.trim());				
					j2ksec	= Util.dateToJ2K(date);
				} catch (ParseException e) {
					logger.log(Level.SEVERE, "Skipping line " + lineNumber + " (timestamp parse error)");
					line	= rr.nextLine();
					lineNumber++;
					continue;
				}
				
				ColumnValue tsColumn = new ColumnValue("j2ksec", j2ksec);
				
				// iterate through each data source that was defined and assign data from this line to it
				HashMap<Integer, String> dsColumnMap;
				for (int i = 0; i < dataSourceList.size(); i++) {
					dataSource		= dataSourceList.get(i);
					sqlDataSource	= sqlDataSourceMap.get(dataSource);
					
					// columns for this data source
					columns			= dataSourceColumnMap.get(dataSource);
					columnArray		= columns.split(",");
					dsColumnMap		= new HashMap<Integer, String>();
					for (int j = 0; j < columnArray.length; j++) {
						dsColumnMap.put(j, columnArray[j]);
					}
					
					// channel for this data source
					if (sqlDataSource.getChannelsFlag()) {
						channel = sqlDataSource.defaultGetChannel(channelCode, sqlDataSource.getChannelTypesFlag());
						if (channel == null) {
							channel = new Channel("0:" + channelCode);
							sqlDataSource.defaultCreateChannel(channel, 
									sqlDataSource.getChannelsFlag(), sqlDataSource.getTranslationsFlag(), 
									sqlDataSource.getRanksFlag(), sqlDataSource.getColumnsFlag());
						}
					}
					
					// rank for this data source
					if (sqlDataSource.getRanksFlag()) {
						rid	= sqlDataSource.defaultGetRankID(rank.getRank());
					} else {
						rid	= 0;
					}
					
					// create a data entry map for this data source, with the columns that it wants
					HashMap<Integer, ColumnValue> dataSourceEntryMap = new HashMap<Integer, ColumnValue>();
					count	= 0;
					dataSourceEntryMap.put(count, tsColumn);
					count++;
					
					// store the remaining data columns
					for (int j = 0; j < columnValueMap.size(); j++) {
						columnValue = columnValueMap.get(j);
						name		= columnValue.columnName;
						value		= columnValue.columnValue;
						for (int k = 0; k < dsColumnMap.size(); k++) {
							if (name.equals(dsColumnMap.get(k))) {
								dataSourceEntryMap.put(count, columnValue);
								count++;
							}
						}
					}
					
					// put the list of entries a double matrix and create a column names array
					DoubleMatrix2D dm		= DoubleFactory2D.dense.make(1, dataSourceEntryMap.size());
					String[] columnNames	= new String[dataSourceEntryMap.size()];
					String lineOutput		= "";
					for (int j = 0; j < dataSourceEntryMap.size(); j++) {
						columnValue		= dataSourceEntryMap.get(j);
						name			= columnValue.columnName;
						value			= columnValue.columnValue;
						columnNames[j]	= name;
						dm.setQuick(0, j, value);
						
						if (name.equals("j2ksec")) {
							lineOutput	= lineOutput + Util.j2KToDateString(value) + "(" + value + ")/";
						} else {
							lineOutput	= lineOutput + name + "=" + value + "/";
						}
					}
					logger.log(Level.INFO, dataSource + " line:" + lineNumber + " " + lineOutput);
					
					// assign the double matrix and column names to a generic data matrix
					gdm = new GenericDataMatrix(dm);
					gdm.setColumnNames(columnNames);
					
					// insert the data to the database
					sqlDataSource.defaultInsertData(
							channelCode, gdm, sqlDataSource.getTranslationsFlag(), sqlDataSource.getRanksFlag(), rid);
				}
				
				line	= rr.nextLine();
				lineNumber++;
			}
		
		// catch exceptions
		} catch (Exception e) {
			logger.log(Level.SEVERE, "ImportFile.process(" + filename + ") failed.", e);
		}
	}
	
	/**
	 * Main method
	 * Command line syntax:
	 *  -h, --help print help message
	 *  -c config file name
	 *  -v verbose mode
	 *  files ...
	 */
	public static void main(String as[]) {
		
		String configFile;
		boolean verbose;
		Set<String> flags;
		Set<String> keys;
		
		flags	= new HashSet<String>();
		keys	= new HashSet<String>();
		keys.add("-c");
		flags.add("-h");
		flags.add("--help");
		flags.add("-v");
		
		Arguments args = new Arguments(as, flags, keys);
		
		if (args.flagged("-h") || args.flagged("--help")) {
			System.err.println("java gov.usgs.vdx.in.ImportFile [-c configFile]");
			System.exit(-1);
		}
		
		if (!args.contains("-c")) {
			System.err.println("config file required");
			System.exit(-1);
		}
		
		ImportFile importFile	= new ImportFile();

		configFile	= args.get("-c");
		verbose		= args.flagged("-v");
		importFile.initialize(configFile, verbose);

		List<String> files	= args.unused();
		for (String file : files) {
			importFile.process(file);
		}
	}
}

