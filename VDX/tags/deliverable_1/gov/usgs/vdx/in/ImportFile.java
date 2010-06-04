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
import gov.usgs.util.CurrentTime;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.Channel;
import gov.usgs.vdx.data.Column;
import gov.usgs.vdx.data.GenericDataMatrix;
import gov.usgs.vdx.data.Rank;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.vdx.data.SQLDataSourceHandler;

import cern.colt.matrix.*;

/**
 * Import files
 *  
 * @author Loren Antolik
 */
public class ImportFile implements Importer {
	
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
	public Map<Integer, String> defaultImportColumnMap;
	
	public String dataSource;
	public SQLDataSource sqlDataSource;
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

	public Channel channel;	
	public String channelCode, channelName;
	public double channelLon, channelLat, channelHeight, channelAzimuth;
	public List<String> channelList;
	public Map<String, Channel> channelMap;
	public String channels;
	public String[] channelArray;
	public String[] dsChannelArray;
	public String defaultChannels;
	
	public String channelCols;
	public Map<String, String> channelColumnMap;
	
	public Column column;
	public String columnName, columnDescription, columnUnit;
	public int columnIdx;
	public boolean columnActive, columnChecked;
	public List<String> columnList;
	public HashMap<String, Column> columnMap;
	public Map<String, Column> dbColumnMap;
	public String columns;
	public String[] columnArray;	
	public String defaultColumns;

	public double azimuthNom;
	public double azimuthInst;
	
	public String importerType;	
	public Logger logger;
	
	// timing output
	public CurrentTime currentTime = CurrentTime.getInstance();
	
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
		logger.log(Level.INFO, "ImportFile.initialize() succeeded.");
		
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
			logger.log(Level.SEVERE, "%s was not successfully read", configFile);
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
		
		// ImportFile specific directives
		filenameMask	= Util.stringToString(params.getString("filenameMask"), "");
		headerLines		= Util.stringToInt(params.getString("headerLines"), 0);
		
		// information related to the timestamps
		timestampMask	= Util.stringToString(params.getString("timestampMask"), "yyyy-MM-dd HH:mm:ss");
		timeZone		= Util.stringToString(params.getString("timezone"), "GMT");
		dateIn			= new SimpleDateFormat(timestampMask);
		dateOut			= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateIn.setTimeZone(TimeZone.getTimeZone(timeZone));
		dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));
		
		// get the list of ranks that are being used in this import
		rankParams		= params.getSubConfig("rank");
		rankName		= Util.stringToString(rankParams.getString("name"), "DEFAULT");
		rankValue		= Util.stringToInt(rankParams.getString("value"), 1);
		rankDefault		= Util.stringToInt(rankParams.getString("default"), 0);
		rank			= new Rank(0, rankName, rankValue, rankDefault);
		
		// get the list of columns that are being used in this import
		columnList	= params.getList("column");
		if (columnList != null) {
			dbColumnMap	= new HashMap<String, Column>();
			for (int i = 0; i < columnList.size(); i++) {
				columnName			= columnList.get(i);
				columnParams		= params.getSubConfig(columnName);
				columnIdx			= Util.stringToInt(columnParams.getString("idx"), i);
				columnDescription	= Util.stringToString(columnParams.getString("description"), columnName);
				columnUnit			= Util.stringToString(columnParams.getString("unit"), columnName);
				columnChecked		= Util.stringToBoolean(columnParams.getString("checked"), false);
				columnActive		= Util.stringToBoolean(columnParams.getString("active"), true);
				column 				= new Column(columnIdx, columnName, columnDescription, columnUnit, columnChecked, columnActive);
				dbColumnMap.put(columnName, column);
			}
		}
		
		// information related to a row of data in this import.  be sure to keep these in order!
		importColumns		= params.getString("importColumns");
		if (importColumns == null) {
			logger.log(Level.SEVERE, "importColumns parameter missing from config file");
			System.exit(-1);
		}
		
		// define the default import columns to use for this instance
		defaultColumns			= "";
		importColumnArray		= importColumns.split(",");
		defaultImportColumnMap	= new HashMap<Integer, String>();
		for (int i = 0; i < importColumnArray.length; i++) {
			columnName		= importColumnArray[i].trim();
			if (!columnName.equals("IGNORE") && !columnName.equals("CHANNEL") && !columnName.equals("TIMESTAMP")) {
				defaultColumns += columnName + ",";
			}
			defaultImportColumnMap.put(i, columnName);
		}
		defaultColumns	= defaultColumns.substring(0, defaultColumns.length() - 1);
		
		// double check to make sure the user entered in data column name, and not pre-defined keywords only
		if (defaultColumns.length() == 0) {
			logger.log(Level.SEVERE, "importColumns parameter does not contain any data columns");
			System.exit(-1);
		}
		
		// get the list of channels that are being used in this import
		defaultChannels		= "";
		channelList			= params.getList("channel");
		channelMap			= new HashMap<String, Channel>();
		channelColumnMap	= new HashMap<String, String>();
		if (channelList != null) {
			for (int i = 0; i < channelList.size(); i++) {
				
				// channel configuration
				channelCode		= channelList.get(i);
				
				// default channels
				defaultChannels+= channelCode + ",";
				
				// channel params
				channelParams	= params.getSubConfig(channelCode);
				
				// settings related to this channel
				channelName		= Util.stringToString(channelParams.getString("name"), channelCode);
				channelLon		= Util.stringToDouble(channelParams.getString("longitude"), Double.NaN);
				channelLat		= Util.stringToDouble(channelParams.getString("latitude"), Double.NaN);
				channelHeight	= Util.stringToDouble(channelParams.getString("height"), Double.NaN);
				channel			= new Channel(0, channelCode, channelName, channelLon, channelLat, channelHeight);
				channelMap.put(channelCode, channel);
				
				// channel columns
				channelCols		= channelParams.getString("columns");
				if (channelCols == null) {
					channelCols	= importColumns;
				}
				channelColumnMap.put(channelCode, channelCols);
			}
			defaultChannels	= defaultChannels.substring(0, defaultChannels.length() - 1);
		}
		
		// get the list of data sources that are being used in this import
		dataSourceList	= params.getList("dataSource");
		if (dataSourceList == null) {
			logger.log(Level.SEVERE, "dataSource parameter(s) missing from config file");
			System.exit(-1);
			
		}
		
		// define the data source handler that acts as a wrapper for data sources
		sqlDataSourceHandler	= new SQLDataSourceHandler(driver, url, prefix);
		sqlDataSourceMap		= new HashMap<String, SQLDataSource>();
		dataSourceChannelMap	= new HashMap<String, String>();
		dataSourceColumnMap		= new HashMap<String, String>();
		dataSourceRankMap		= new HashMap<String, Integer>();
		
		// iterate through each of the data sources and setup the db for it
		for (int i = 0; i < dataSourceList.size(); i++) {
			
			// get the data source name and define the columns that it contains
			dataSource			= dataSourceList.get(i);
			dataSourceParams	= params.getSubConfig(dataSource);
			
			// get the columns for this data source
			columns				= dataSourceParams.getString("columns");
			if (columns == null) {
				logger.log(Level.WARNING, dataSource + " columns not defined. all available columns will be imported");
				columns			= defaultColumns;
			}
			dataSourceColumnMap.put(dataSource, columns);
			
			// get the channels for this data source
			channels			= dataSourceParams.getString("channels");
			if (channels == null) {
				logger.log(Level.WARNING, dataSource + " channels not defined.  all available channels will be imported");
				channels		= "";
				// channels		= defaultChannels;
			}
			dataSourceChannelMap.put(dataSource, channels);
			
			// lookup the data source from the list that is in vdxSources.config
			sqlDataSourceDescriptor	= sqlDataSourceHandler.getDataSourceDescriptor(dataSource);
			if (sqlDataSourceDescriptor == null) {
				logger.log(Level.SEVERE, dataSource + " sql data source does not exist in vdxSources.config");
				continue;
			}
			
			// formally get the data source from the list of descriptors.  this will initialize the data source which includes db creation
			sqlDataSource	= sqlDataSourceDescriptor.getSQLDataSource();
			
			// store the reference to the initialized data source in the map of initialized data sources
			sqlDataSourceMap.put(dataSource, sqlDataSource);
			
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
				dataSourceRankMap.put(dataSource, tempRank.getId());
			}
				
			// create columns entries
			if (sqlDataSource.getColumnsFlag()) {
				columnArray	= columns.split(",");
				for (int j = 0; j < columnArray.length; j++) {
					columnName	= columnArray[j];
					if (sqlDataSource.defaultGetColumn(columnName) == null) {
						column	= dbColumnMap.get(columnName);
						if (column == null) {
							column	= new Column(1, columnName, columnName, columnName, false);
						}
						sqlDataSource.defaultInsertColumn(column);
					}
				}
			}
			
			// create translations table which is based on column entries
			if (sqlDataSource.getTranslationsFlag()) {
				sqlDataSource.defaultCreateTranslation();
			}

			// create channels tables
			if (sqlDataSource.getChannelsFlag() && channels.length() > 0) {
				channelArray = channels.split(",");
				
				for (int j = 0; j < channelArray.length; j++) {
					channelCode		= channelArray[j];
					channel 		= channelMap.get(channelCode);
					channelParams	= params.getSubConfig(channelCode);
					
					// if the channel doesn't exist then create it with the default tid of 1
					if (sqlDataSource.defaultGetChannel(channel.getCode(), sqlDataSource.getChannelTypesFlag()) == null) {
						if (sqlDataSource.getType().equals("tilt")) {
							azimuthNom	= Util.stringToDouble(channelParams.getString("azimuth"), 0);
							sqlDataSource.defaultCreateTiltChannel(channel, 1, azimuthNom, 
								sqlDataSource.getChannelsFlag(), sqlDataSource.getTranslationsFlag(), 
								sqlDataSource.getRanksFlag(), sqlDataSource.getColumnsFlag());
						} else {
							sqlDataSource.defaultCreateChannel(channel, 1, 
								sqlDataSource.getChannelsFlag(), sqlDataSource.getTranslationsFlag(), 
								sqlDataSource.getRanksFlag(), sqlDataSource.getColumnsFlag());
						}
						
						// retrieve the new channel and store it off
						channel	= sqlDataSource.defaultGetChannel(channel.getCode(), sqlDataSource.getChannelTypesFlag());
						channelMap.put(channelArray[i], channel);
					}
					
					// create a new translation if any non-default values were specified, and use the new tid for the create channel statement
					if (sqlDataSource.getTranslationsFlag()) {
						int tid			= 1;
						int extraColumn	= 0;
						double multiplier, offset;
					
						// get the translations sub config for this channel
						translationParams		= channelParams.getSubConfig("translation"); 
						List<Column> columnList	= sqlDataSource.defaultGetColumns(true, false);
						
						// apply an offset if this is a tilt data source to include the installation azimuth
						if (sqlDataSource.getType().equals("tilt")) {
							extraColumn	= 1;
						}
						
						// create a matrix to store the translation data
						DoubleMatrix2D dm		= DoubleFactory2D.dense.make(1, columnList.size() * 2 + extraColumn);
						String[] columnNames	= new String[columnList.size() * 2 + extraColumn];
						
						// save the installation azimuth if this is a tilt data source
						if (sqlDataSource.getType().equals("tilt")) {
							azimuthInst	= Util.stringToDouble(translationParams.getString("azimuth"), 0);
							dm.setQuick(0, columnList.size() * 2, azimuthInst);
							columnNames[columnList.size() * 2]	= "azimuth";
						}
						
						// iterate through the column list to get the translation values
						for (int k = 0; k < columnList.size(); k++) {
							column		= columnList.get(k);
							columnName	= column.name;
							multiplier	= Util.stringToDouble(translationParams.getString("c" + columnName), 1);
							offset		= Util.stringToDouble(translationParams.getString("d" + columnName), 0);
							dm.setQuick(0, k * 2, multiplier);
							dm.setQuick(0, k * 2 + 1, offset);
							columnNames[k * 2]		= "c" + columnName;
							columnNames[k * 2 + 1]	= "d" + columnName;
						}
						
						GenericDataMatrix gdm = new GenericDataMatrix(dm);
						gdm.setColumnNames(columnNames);
						
						tid	= sqlDataSource.defaultGetTranslation(channel.getCode(), gdm);
						if (tid == 1) {
							tid	= sqlDataSource.defaultInsertTranslation(channel.getCode(), gdm);
						}							
						if (tid != sqlDataSource.defaultGetChannelTranslationID(channel.getCode())) {
							sqlDataSource.defaultUpdateChannelTranslationID(channel.getCode(), tid);
						}
					}
				}
			}
		}
	}
	
	/**
	 * Parse file from url (resource locator or file name)
	 * @param filename
	 */
	public void process(String filename) {
		
		try {
			
			// check that the file exists
			ResourceReader rr = ResourceReader.getResourceReader(filename);
			if (rr == null) {
				logger.log(Level.SEVERE, "skipping: " + filename + " (resource is invalid)");
				return;
			}
			
			// make a short file name.  we'll use this later on
			String shortFilename = filename.substring(filename.lastIndexOf("/") + 1);
			
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
				
				// reset the channel code, as it will be derived from the filename, and not the config file, or the contents of the file
				channelCode				= "";
				
				// filename mask can be shorter than the filename, but not longer
				if (filenameMask.length() > shortFilename.length()) {
					logger.log(Level.SEVERE, "skipping: " + filename + " (bad filename mask)");
					return;
				}
				
				// build up the channel code from the mask
				for (int i = 0; i < filenameMask.length(); i++) {
					if (String.valueOf(filenameMask.charAt(i)).equals("C")) {
						channelCode	= channelCode + String.valueOf(shortFilename.charAt(i));
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
				
				// split the data row into an ordered list. be sure to use the two argument split, as some lines may have many trailing delimiters
				String[] valueArray					= line.split(",", -1);
				HashMap<Integer, String> valueMap	= new HashMap<Integer, String>();
				for (int i = 0; i < valueArray.length; i++) {
					valueMap.put(i, valueArray[i].trim());
				}
				
				// set the import column map to the default import column map
				importColumnMap	= defaultImportColumnMap;
				
				// make sure the data row matches the defined data columns
				if (importColumnMap.size() > valueMap.size()) {
					logger.log(Level.SEVERE, "Skipping line " + lineNumber + " (too few values)");
					lineNumber++;
					continue;
				}
				
				// map the columns to the values.  look for the TIMESTAMP and CHANNEL flags
				HashMap<Integer, ColumnValue> columnValueMap = new HashMap<Integer, ColumnValue>();
				ColumnValue columnValue;
				String name;
				double value;
				int count		= 0;
				String tsValue	= "";
				
				// look at the contents of the line quickly and see if the channel is defined
				for (int i = 0; i < importColumnMap.size(); i++) {
					name	= importColumnMap.get(i);
					if (name.equals("CHANNEL")) {
						channelCode	= valueMap.get(i);
						break;
					}
				}
				
				// lookup the columns from the channel column map for this channel
				channelCols	= channelColumnMap.get(channelCode);
				if (channelCols != null) {
					importColumnArray	= channelCols.split(",");
					importColumnMap		= new HashMap<Integer, String>();
					for (int i = 0; i < importColumnArray.length; i++) {
						importColumnMap.put(i, importColumnArray[i].trim());
					}
				}
				
				// do you the actual parsing of the data using the custom column map for this channel if it existed
				for (int i = 0; i < importColumnMap.size(); i++) {
					
					name		= importColumnMap.get(i);
					
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
					channelCode = channelCode.replace('\\', '$').replace('/', '$').replace('.', '$').replace(' ', '$');;
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
					String timestamp	= tsValue.trim();
					date				= dateIn.parse(timestamp);				
					j2ksec				= Util.dateToJ2K(date);
				} catch (ParseException e) {
					logger.log(Level.SEVERE, "Skipping line " + lineNumber + " (timestamp parse error)");
					line	= rr.nextLine();
					lineNumber++;
					continue;
				}
				
				ColumnValue tsColumn = new ColumnValue("j2ksec", j2ksec);
				
				// iterate through each data source that was defined and assign data from this line to it
				dsIterator			= dataSourceList.iterator();		
				while (dsIterator.hasNext()) {
					
					// get the data source name and it's associated sql data source
					dataSource		= dsIterator.next();
					sqlDataSource	= sqlDataSourceMap.get(dataSource);
					
					// check that the sql data source was initialized properly above
					if (sqlDataSource == null) {
						logger.log(Level.SEVERE, "Skipping dataSource " + dataSource + " for line " + lineNumber);
						continue;
					}
					
					// lookup in the channels map to see if we are filtering on stations
					boolean	channelMemberOfDataSource = false;
					channels	= dataSourceChannelMap.get(dataSource);
					if (channels.length() > 0) {
						channelArray	= channels.split(",");
						for (int j = 0; j < channelArray.length; j++) {
							if (channelCode.equals(channelArray[j])) {
								channelMemberOfDataSource = true;
								continue;
							}
						}
						if (!channelMemberOfDataSource) {
							logger.log(Level.SEVERE, "Skipping line " + lineNumber + " dataSource " + dataSource + " (" + channelCode + " not a member of dataSource)");
							continue;
						}
					}
					
					// channel for this data source.  create it if it doesn't exist
					if (sqlDataSource.getChannelsFlag()) {
						if (sqlDataSource.defaultGetChannel(channelCode, sqlDataSource.getChannelTypesFlag()) == null) {
							sqlDataSource.defaultCreateChannel(new Channel(0, channelCode, null, Double.NaN, Double.NaN, Double.NaN), 1,
									sqlDataSource.getChannelsFlag(), sqlDataSource.getTranslationsFlag(), 
									sqlDataSource.getRanksFlag(), sqlDataSource.getColumnsFlag());
						}
					}
					
					// columns for this data source
					columns			= dataSourceColumnMap.get(dataSource);
					columnArray		= columns.split(",");
					HashMap<Integer, String> dsColumnMap	= new HashMap<Integer, String>();
					for (int j = 0; j < columnArray.length; j++) {
						dsColumnMap.put(j, columnArray[j]);
					}
					
					// rank for this data source.  this should already exist in the database
					if (sqlDataSource.getRanksFlag()) {
						rid	= dataSourceRankMap.get(dataSource);
					} else {
						rid	= 1;
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
					for (int j = 0; j < dataSourceEntryMap.size(); j++) {
						columnValue		= dataSourceEntryMap.get(j);
						name			= columnValue.columnName;
						value			= columnValue.columnValue;
						columnNames[j]	= name;
						dm.setQuick(0, j, value);
					}
					
					// assign the double matrix and column names to a generic data matrix
					GenericDataMatrix gdm = new GenericDataMatrix(dm);
					gdm.setColumnNames(columnNames);
					
					// insert the data to the database
					sqlDataSource.defaultInsertData(channelCode, gdm, sqlDataSource.getTranslationsFlag(), sqlDataSource.getRanksFlag(), rid);
				}
				
				line	= rr.nextLine();
				lineNumber++;
			}
		
		// catch exceptions
		} catch (Exception e) {
			logger.log(Level.SEVERE, "ImportFile.process(" + filename + ") failed.", e);
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
		
		ImportFile importer	= new ImportFile();
		
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

