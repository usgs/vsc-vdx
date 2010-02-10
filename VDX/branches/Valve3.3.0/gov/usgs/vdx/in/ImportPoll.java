package gov.usgs.vdx.in;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.util.CurrentTime;
import gov.usgs.vdx.data.Channel;
import gov.usgs.vdx.data.Column;
import gov.usgs.vdx.data.GenericDataMatrix;
import gov.usgs.vdx.data.Rank;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.vdx.data.SQLDataSourceHandler;

import cern.colt.matrix.*;

/**
 * A program to poll data from a collection of ip devices and put it in the database
 * 
 * @author Loren Antolik
 */
public class ImportPoll extends Poller implements Importer {
	
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
	public SQLDataSource sqlDataSource;
	public SQLDataSourceHandler sqlDataSourceHandler;
	public SQLDataSourceDescriptor sqlDataSourceDescriptor;	
	public List<String> dataSourceList;
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
	public double channelLon, channelLat, channelHeight;
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
	
	public CurrentTime currentTime = CurrentTime.getInstance();
	
	public String importerType;
	
	public Logger logger;

	public double azimuthNom;
	public double azimuthInst;

	public String deviceIP;
	public int devicePort;
	public int postConnectDelay;
	public int betweenPollDelay;
	
	public int callNumber;
	public int repeater;
	public int dataLines;
	
	public int connTimeout;
	public int dataTimeout;
	public int maxRetries;
	public String timeSource;
	public String instrument;
	public String delimiter;
	public int tiltid;	
	public Map<String, ConnectionSettings> settingMap;
	public ConnectionSettings settings;	
	public FreewaveIPConnection connection;
	
	static {
		flags	= new HashSet<String>();
		keys	= new HashSet<String>();
		keys.add("-c");
		flags.add("-h");
		flags.add("-v");
	}
	
	public String timeDataSource = null;

	/**
	 * takes a config file as a parameter and parses it to prepare for importing
	 * @param cf configuration file
	 * @param verbose true for info, false for severe
	 */
	public void initialize(String importerClass, String configFile, boolean verbose) {
		
		// initialize the logger for this importer
		logger	= Logger.getLogger(importerClass);
		logger.log(Level.INFO, "ImportPoll.initialize() succeeded.");
		
		// process the config file
		processConfigFile(configFile);
	}
	
	/** 
	 * default constructor
	 */
	public ImportPoll () {
		super();
		nextInterval = 0;
	}
	
	/**
	 * Parse configuration file.  This sets class variables used in the importing process
	 * @param configFile	name of the config file
	 */
	public void processConfigFile(String configFile) {
		
		logger.log(Level.INFO, "Reading config file " + configFile);
		
		// instantiate the config file
		params		= new ConfigFile(configFile);
		
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
		
		// information related to the timestamps
		timestampMask	= Util.stringToString(params.getString("timestampMask"), "yyyy-MM-dd HH:mm:ss");
		timeZone		= Util.stringToString(params.getString("timezone"), "GMT");
		dateIn			= new SimpleDateFormat(timestampMask);
		dateOut			= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		dateIn.setTimeZone(TimeZone.getTimeZone(timeZone));
		dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));
		
		// get the list of ranks that are being used in this import
		rankParams		= params.getSubConfig("rank");
		rankName		= Util.stringToString(rankParams.getString("name"), "DEFAULT");
		rankValue		= Util.stringToInt(rankParams.getString("value"), 1);
		rankDefault		= Util.stringToInt(rankParams.getString("default"), 0);
		rank			= new Rank(0, rankName, rankValue, rankDefault);
		
		// get connection settings related to this instance
		deviceIP			= params.getString("deviceIP");
		devicePort			= Util.stringToInt(params.getString("devicePort"));
		postConnectDelay	= Util.stringToInt(params.getString("postConnectDelay"), 5000);	
		betweenPollDelay	= Util.stringToInt(params.getString("betweenPollDelay"), 5000);	
		
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
		defaultColumns		= "";
		importColumnArray	= importColumns.split(",");
		for (int i = 0; i < importColumnArray.length; i++) {
			columnName		= importColumnArray[i].trim();
			if (!columnName.equals("IGNORE") && !columnName.equals("CHANNEL") && !columnName.equals("TIMESTAMP")) {
				defaultColumns += columnName + ",";
			}
		}
		defaultColumns	= defaultColumns.substring(0, defaultColumns.length() - 1);
		
		// double check to make sure the user entered in data column name, and not pre-defined keywords only
		if (defaultColumns.length() == 0) {
			logger.log(Level.SEVERE, "importColumns parameter does not contain any data columns");
			System.exit(-1);
		}
		
		// get the list of channels that are being used in this import
		defaultChannels	= "";
		channelList		= params.getList("channel");
		if (channelList != null) {
			channelMap			= new HashMap<String, Channel>();
			settingMap			= new HashMap<String, ConnectionSettings>();
			channelColumnMap	= new HashMap<String, String>();
			for (int i = 0; i < channelList.size(); i++) {
				
				// channel configuration
				channelCode		= channelList.get(i);
				
				// default channels
				defaultChannels+= channelCode + ",";
				
				// channel params
				channelParams	= params.getSubConfig(channelCode);
				
				// channel related settings
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
				
				// channel connection related settings
				callNumber		= Util.stringToInt(channelParams.getString("callNumber"));
				repeater		= Util.stringToInt(channelParams.getString("repeater"), 0);
				dataLines		= Util.stringToInt(channelParams.getString("dataLines"), 50);
				connTimeout		= Util.stringToInt(channelParams.getString("connTimeout"), 50000);	
				dataTimeout		= Util.stringToInt(channelParams.getString("dataTimeout"), 50000);
				maxRetries		= Util.stringToInt(channelParams.getString("maxRetries"), 2);
				timeSource		= Util.stringToString(channelParams.getString("timeSource"), "tilt");
				instrument		= Util.stringToString(channelParams.getString("instrument"), "zeno");
				delimiter		= Util.stringToString(channelParams.getString("delimiter"), ",");
				tiltid			= Util.stringToInt(channelParams.getString("tiltid"), 0);
				settings		= new ConnectionSettings(callNumber, repeater, dataLines, connTimeout, dataTimeout, maxRetries, timeSource, instrument, delimiter, tiltid);
				settingMap.put(channelCode, settings);
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
			
			// look up to see if this is the time data source
			String timesource	= dataSourceParams.getString("timesource");
			if (timesource != null && timesource.equals("true")) {
				timeDataSource	= dataSource;
			}
			
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
				channels		= defaultChannels;
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
		
		if (timeDataSource == null) {
			logger.log(Level.SEVERE, "dataSource time source not specified");
			System.exit(-1);
		}
		
		// output the connection settings for the config file
		logger.log(Level.INFO, "");
		logger.log(Level.INFO, "###### CONNECTION SETTINGS ######");
		logger.log(Level.INFO, "###### MASTER ######");
		logger.log(Level.INFO, outputConnectionHeader());
		logger.log(Level.INFO, outputConnectionInfo());
		channelArray = defaultChannels.split(",");
		for (int c = 0; c < channelArray.length; c++) {	
			channel		= channelMap.get(channelArray[c]);
			settings	= settingMap.get(channelArray[c]);
			logger.log(Level.INFO, "###### " + channel.getCode() + " ######");
			logger.log(Level.INFO, settings.headerString());
			logger.log(Level.INFO, settings.toString());
		}
		logger.log(Level.INFO, "");
	}
	
	/** 
	 * this is where the program lives most of the time, except for startup
	 */	
	public void poll() {
		
		try {
			
			// output initial polling message
			logger.log(Level.INFO, "");
			logger.log(Level.INFO, "BEGIN POLLING CYCLE");
			
			// create an array of all the channels used in this polling process
			channelArray = defaultChannels.split(",");
			
			// iterate through each of the channels
			for (int c = 0; c < channelArray.length; c++) {
				
				// define the channel and settings for this instance
				channel		= channelMap.get(channelArray[c]);
				channelCode	= channel.getCode();
				settings	= settingMap.get(channelCode);				
				channelCols	= channelColumnMap.get(channelCode);
				
				// get the import line definition for this channel
				importColumnArray	= channelCols.split(",");
				importColumnMap		= new HashMap<Integer, String>();
				for (int i = 0; i < importColumnArray.length; i++) {
					importColumnMap.put(i, importColumnArray[i].trim());
				}
				
				// get the latest data time from the tilt database
				sqlDataSource		= sqlDataSourceMap.get(timeDataSource);
				Date lastDataTime	= sqlDataSource.defaultGetLastDataTime(channelCode);
				if (lastDataTime == null) {
					lastDataTime = new Date(0);
				}
				
				// display logging information
				logger.log(Level.INFO, "");
				logger.log(Level.INFO, "Begin Polling [" + channelCode + "] (lastDataTime: " + dateOut.format(lastDataTime) + ")");
				
				// default some variables used in the loop
				int tries		= 1;
				boolean done	= false;
				
				// iterate through the maximum number of retries as specified in the config file
				while (tries < settings.maxRetries + 1 && !done) {
					
					String line		= "";
					int lineNumber	= 1;	
					
					// try to create a connection to the station
					try {
						
						// instantiate the connection out of the observatory
						if (connection == null) {
							connection = new FreewaveIPConnection(deviceIP, devicePort, settings.dataTimeout);
						}
						
						// connect to the station
						connection.connect(settings.repeater, settings.callNumber, settings.connTimeout);
						Thread.sleep(postConnectDelay);
						CCSAILMessage ccMsg	= new CCSAILMessage(settings.tiltid);
						
						// build the data request string
						String dataRequest	= ccMsg.makeDA(lastDataTime, settings.dataLines);
						connection.writeString(dataRequest);
						
						// get the response from the instrument
						String reply		= connection.getMsg(settings.dataTimeout);
						String replyString	= ccMsg.getMsg(reply, true);
						
						// parse the response by lines
						StringTokenizer st	= new StringTokenizer(replyString, "\n");
						
						// iterate through each line
						while (st.hasMoreTokens()) {
							
							// if this is a valid message, then parse it
							line = st.nextToken();
							
							// validate that this is a line with data
							if (line.startsWith("EOF") || line.startsWith("#") || line.length() <= 1) {
								continue;
							}
							
							// split the data row into an ordered list. be sure to use the two argument split, as some lines may have many trailing delimiters
							String[] valueArray					= line.split(settings.delimiter, -1);
							HashMap<Integer, String> valueMap	= new HashMap<Integer, String>();
							for (int i = 0; i < valueArray.length; i++) {
								valueMap.put(i, valueArray[i].trim());
							}
							
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
									tsValue	+= valueMap.get(i) + " ";
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
							
							// make sure that the timestamp has something in it
							if (tsValue.length() == 0) {
								logger.log(Level.SEVERE, "Skipping line " + lineNumber + " (timestamp not found)");
								continue;
							}
							
							// convert the timezone of the input date and convert to j2ksec
							try {
								String timestamp	= tsValue.trim();
								date				= dateIn.parse(timestamp);				
								j2ksec				= Util.dateToJ2K(date);
							} catch (ParseException e) {
								logger.log(Level.SEVERE, "Skipping line " + lineNumber + " (timestamp parse error)");
								continue;
							}
							
							ColumnValue tsColumn = new ColumnValue("j2ksec", j2ksec);
							
							// iterate through each data source that was defined and assign data from this line to it
							for (int i = 0; i < dataSourceList.size(); i++) {
								
								// get the data source name and it's associated sql data source
								dataSource		= dataSourceList.get(i);
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
									dsChannelArray	= channels.split(",");
									for (int j = 0; j < dsChannelArray.length; j++) {
										if (channelCode.equals(dsChannelArray[j])) {
											channelMemberOfDataSource = true;
											continue;
										}
									}
									if (!channelMemberOfDataSource) {
										// logger.log(Level.SEVERE, "Skipping line " + lineNumber + " dataSource " + dataSource + " (" + channelCode + " not a member of dataSource)");
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
								
								// VALVE2 HACK
								double j2ksec, xTilt, yTilt, holeTemp, boxTemp, instVolt, gndVolt, rainfall, dt01, dt02, barometer, co2l, co2h;
								DoubleMatrix2D temp;
								if (dataSource.equals("hvo_deformation_tilt") && sqlDataSource.getType().equals("tilt")) {
									temp 		= gdm.getColumn("j2ksec");
									j2ksec		= temp.getQuick(0, 0);
									temp 		= gdm.getColumn("xTilt");
									xTilt		= temp.getQuick(0, 0);
									temp 		= gdm.getColumn("yTilt");
									yTilt		= temp.getQuick(0, 0);
									temp 		= gdm.getColumn("holeTemp");
									holeTemp	= temp.getQuick(0, 0);
									temp 		= gdm.getColumn("boxTemp");
									boxTemp		= temp.getQuick(0, 0);
									temp 		= gdm.getColumn("instVolt");
									instVolt	= temp.getQuick(0, 0);
									temp 		= gdm.getColumn("gndVolt");
									gndVolt		= temp.getQuick(0, 0);
									temp 		= gdm.getColumn("rainfall");
									rainfall	= temp.getQuick(0, 0);
									sqlDataSource.insertV2TiltData(channelCode.toLowerCase(), j2ksec, xTilt, yTilt, holeTemp, boxTemp, instVolt, gndVolt, rainfall);
								} else if (dataSource.equals("hvo_deformation_strain") && sqlDataSource.getType().equals("genericfixed")) {
									temp 		= gdm.getColumn("j2ksec");
									j2ksec		= temp.getQuick(0, 0);
									temp		= gdm.getColumn("dt01");
									dt01		= temp.getQuick(0, 0);
									temp		= gdm.getColumn("dt02");
									dt02		= temp.getQuick(0, 0);
									temp		= gdm.getColumn("barometer");
									barometer	= temp.getQuick(0, 0);
									sqlDataSource.insertV2StrainData(channelCode.toLowerCase(), j2ksec, dt01, dt02, barometer);
								} else if (dataSource.equals("hvo_gas_co2") && sqlDataSource.getType().equals("genericfixed")) {
									temp 		= gdm.getColumn("j2ksec");
									j2ksec		= temp.getQuick(0, 0);
									temp 		= gdm.getColumn("co2l");
									co2l		= temp.getQuick(0, 0);
									temp 		= gdm.getColumn("co2h");
									co2h		= temp.getQuick(0, 0);
									sqlDataSource.insertV2GasData(1, j2ksec, co2l);
									sqlDataSource.insertV2GasData(2, j2ksec, co2h);
								}
							}
							
							lineNumber++;
						}
						
						// if we made it here then no exceptions were thrown, and we got the data
						done = true;

					} catch (Exception e) {
						logger.log(Level.SEVERE, "ImportPoll.process() failed for channel " + channel.getCode() + " try " + tries, e);
						
					// disconnect from the device, we will reconnect if we are trying again
					} finally {
						if (connection != null) {
							connection.disconnect();
						}
					}
					
					// update the number of tries so we don't waste all day trying
					tries++;
				}
				
				// output a status message based on how everything went above
				if (done) {					
					logger.log(Level.INFO, "End Polling [" + channelCode + "] Success");
				} else {				
					logger.log(Level.INFO, "End Polling [" + channelCode + "] Failure");
				}
				
				// sleep before accessing the next station
				Thread.sleep(betweenPollDelay);
			}
			
			logger.log(Level.INFO, "");
			logger.log(Level.INFO, "END POLLING CYCLE");
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "DataPoller.process() failed.", e);
		}
	}
	
	public String outputConnectionInfo() {
		return deviceIP + "|" + devicePort + "|" + postConnectDelay + "|" + betweenPollDelay;
	}
	
	public String outputConnectionHeader() {
		return "deviceIP|devicePort|postConnectDelay|betweenPollDelay";
	}
	
	public void outputInstructions(String importerClass, String message) {
		if (message == null) {
			System.err.println(message);
		}
		System.err.println(importerClass + " -c configfile");
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
		
		ImportPoll importer	= new ImportPoll();
		
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
		
		importer.startPolling();
	}	
}