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
 * A program to stream data from an ip device and put it in the database
 *
 * @author Loren Antolik
 */
public class ImportStream implements Importer {
	
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
	public SQLDataSource sqlDataSource;
	public SQLDataSourceHandler sqlDataSourceHandler;
	public SQLDataSourceDescriptor sqlDataSourceDescriptor;	
	public List<String> dataSourceList;
	public Map<String, String> dataSourceColumnMap;
	public Map<String, String> dataSourceChannelMap;
	public Map<String, SQLDataSource> sqlDataSourceMap;
	public Iterator<String> dsIterator;
	
	public Rank rank;
	public String rankName;
	public int rankValue, rankDefault;
	
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
	
	public CurrentTime currentTime = CurrentTime.getInstance();

	public double azimuthNom;
	public double azimuthInst;
	
	public int postConnectDelay;
	public int betweenPollDelay;
	public String deviceIP;
	public int devicePort;
	
	public int connTimeout;
	public int dataTimeout;
	public int maxRetries;
	public String timeSource;
	public int syncInterval;
	public String instrument;
	public String delimiter;
	public int tiltid;	
	public Map<String, ConnectionSettings> settingMap;
	public ConnectionSettings settings;	
	public LilyIPConnection connection;
	
	public String importerType;
	
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
	public void initialize(String importerClass, String configFile, boolean verbose) {// initialize the logger for this importer
		logger	= Logger.getLogger(importerClass);
		logger.log(Level.INFO, "ImportHypoInverse.defaultInitialize() succeeded.");
		
		// process the config file
		processConfigFile(configFile);
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
		
		// get the channel that is being used in this import
		defaultChannels	= "";
		channelCode		= params.getString("channel");
		channelParams	= params.getSubConfig(channelCode);
		channelName		= Util.stringToString(channelParams.getString("name"), channelCode);
		channelLon		= Util.stringToDouble(channelParams.getString("lon"), Double.NaN);
		channelLat		= Util.stringToDouble(channelParams.getString("lat"), Double.NaN);
		channelHeight	= Util.stringToDouble(channelParams.getString("height"), Double.NaN);
		channel			= new Channel(0, channelCode, channelName, channelLon, channelLat, channelHeight);
		channelMap.put(channelCode, channel);
		
		// get connection settings related to this channel
		connTimeout		= Util.stringToInt(channelParams.getString("connTimeout"), 2000);	
		dataTimeout		= Util.stringToInt(channelParams.getString("dataTimeout"), 2000);
		maxRetries		= Util.stringToInt(channelParams.getString("maxRetries"), 2);
		timeSource		= Util.stringToString(channelParams.getString("timeSource"), "tilt");
		syncInterval	= Util.stringToInt(channelParams.getString("syncInterval"), 0);
		instrument		= Util.stringToString(channelParams.getString("instrument"), "lily");
		delimiter		= Util.stringToString(channelParams.getString("delimiter"), ",");
		tiltid			= Util.stringToInt(channelParams.getString("tiltid"), 0);
		settings		= new ConnectionSettings(0, 0, 0, connTimeout, dataTimeout, maxRetries, timeSource, syncInterval, instrument, delimiter, tiltid);
		settingMap.put(channelCode, settings);
		
		// save off the list of default channels
		defaultChannels	= channel.getCode();
		
		// get connection settings related to this instance
		deviceIP			= params.getString("deviceIP");
		devicePort			= Util.stringToInt(params.getString("devicePort"));
		postConnectDelay	= Util.stringToInt(params.getString("postConnectDelay"), 500);	
		
		// information related to a row of data in this import.  be sure to keep these in order!
		importColumns		= params.getString("importColumns");
		if (importColumns == null) {
			logger.log(Level.SEVERE, "importColumns parameter missing from config file");
			System.exit(-1);
		}
		
		importColumnArray	= importColumns.split(",");
		importColumnMap		= new HashMap<Integer, String>();
		for (int i = 0; i < importColumnArray.length; i++) {
			importColumnMap.put(i, importColumnArray[i].trim());
		}
		
		// get the list of columns that are being used in this import
		defaultColumns	= "";
		columnMap		= new HashMap<String, Column>();
		for (int i = 0; i < importColumnMap.size(); i++) {
			columnName			= importColumnMap.get(i);
			if (columnName.equals("IGNORE") || columnName.equals("CHANNEL") || columnName.equals("TIMESTAMP")) {
				continue;
			}
			columnParams		= params.getSubConfig(columnName);
			columnIdx			= Util.stringToInt(columnParams.getString("idx"), i);
			columnDescription	= Util.stringToString(columnParams.getString("description"), columnName);
			columnUnit			= Util.stringToString(columnParams.getString("unit"), columnName);
			columnChecked		= Util.stringToBoolean(columnParams.getString("checked"), false);
			columnActive		= Util.stringToBoolean(columnParams.getString("active"), true);
			column 				= new Column(columnIdx, columnName, columnDescription, columnUnit, columnChecked, columnActive);
			columnMap.put(columnName, column);
			defaultColumns		= defaultColumns + columnName + ",";
		}
		defaultColumns	= defaultColumns.substring(0, defaultColumns.length() - 1);
		
		// double check to make sure the user entered in data column name, and not pre-defined keywords only
		if (defaultColumns.length() == 0) {
			logger.log(Level.SEVERE, "importColumns parameter does not contain any data columns");
			System.exit(-1);
		}
		
		// define the data source handler that acts as a wrapper for data sources
		sqlDataSourceHandler	= new SQLDataSourceHandler(driver, url, prefix);
		sqlDataSourceMap		= new HashMap<String, SQLDataSource>();
		
		// get the list of data sources that are being used in this import
		dataSourceList	= params.getList("dataSource");
		if (dataSourceList != null) {
			dsIterator				= dataSourceList.iterator();
			dataSourceChannelMap	= new HashMap<String, String>();
			dataSourceColumnMap		= new HashMap<String, String>();		
			while (dsIterator.hasNext()) {
				
				// get the data source name and define the columns that it contains
				dataSource			= dsIterator.next();
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
				
				// create rank entry
				if (sqlDataSource.getRanksFlag()) {
					rank = sqlDataSource.defaultInsertRank(rank);
					if (rank == null) {
						logger.log(Level.SEVERE, "invalid rank");
						System.exit(-1);
					}
				}
					
				// create columns entries
				if (sqlDataSource.getColumnsFlag()) {
					columnArray	= columns.split(",");
					for (int i = 0; i < columnArray.length; i++) {
						sqlDataSource.defaultInsertColumn(columnMap.get(columnArray[i]));
					}
				}
				
				// create translations table which is based on column entries
				if (sqlDataSource.getTranslationsFlag()) {
					sqlDataSource.defaultCreateTranslation();
				}
	
				// create channels tables
				if (sqlDataSource.getChannelsFlag() && channels.length() > 0) {
					channelArray = channels.split(",");
					for (int i = 0; i < channelArray.length; i++) {
						
						channel = channelMap.get(channelArray[i]);
						
						// create a new translation if any non-default values were specified, and use the new tid for the create channel statement
						int tid			= 1;
						int extraColumn	= 0;
						int count		= 0;
						double multiplier, offset;
						if (sqlDataSource.getTranslationsFlag()) {
						
							// get the translations sub config for this channel
							channelParams			= params.getSubConfig(channel.getCode());
							translationParams		= channelParams.getSubConfig("translation"); 
							List<Column> columnList	= sqlDataSource.defaultGetColumns(true, false);
							
							// apply an offset if this is a tilt data source to include the installation azimuth
							if (sqlDataSource.getType().equals("tilt")) {
								extraColumn	= 1;
							}
							
							// create a matrix to store the translation data
							DoubleMatrix2D dm		= DoubleFactory2D.dense.make(1, columnList.size() * 2 + extraColumn);
							String[] columnNames	= new String[columnList.size() * 2 + extraColumn];
							
							// iterate through the column list to get the translation values
							for (int j = 0; j < columnList.size() - extraColumn; j++) {
								count		= j;
								column		= columnList.get(j);
								columnName	= column.name;
								multiplier	= Util.stringToDouble(translationParams.getString("c" + columnName), 1.0);
								offset		= Util.stringToDouble(translationParams.getString("d" + columnName), 0.0);
								dm.setQuick(0, j * 2, multiplier);
								dm.setQuick(0, j * 2 + 1, offset);
								columnNames[j * 2]		= "c" + columnName;
								columnNames[j * 2 + 1]	= "d" + columnName;
							}
							
							// save the installation azimuth if this is a tilt data source
							if (sqlDataSource.getType().equals("tilt")) {
								azimuthNom	= Util.stringToDouble(channelParams.getString("azimuthNom"), 0);
								azimuthInst	= Util.stringToDouble(translationParams.getString("azimuthInst"), 0);
								dm.setQuick(0, count * 2 + 2, azimuthInst);
								columnNames[count * 2 + 2]	= "azimuth";
							}
							
							GenericDataMatrix gdm = new GenericDataMatrix(dm);
							gdm.setColumnNames(columnNames);
							tid	= sqlDataSource.defaultInsertTranslation(channel.getCode(), gdm);
							if (tid == -1) {
								tid = 1;
							}
						}
						
						// if the channel exists then update the tid
						if (sqlDataSource.defaultGetChannel(channel.getCode(), sqlDataSource.getChannelTypesFlag()) != null) {
							sqlDataSource.defaultUpdateChannelTranslationID(channel.getCode(), tid);
							
						// if the channel doesn't exist then create it with the tid	
						} else {
							if (sqlDataSource.getType().equals("tilt")) {
								sqlDataSource.defaultCreateTiltChannel(channel, tid, azimuthNom, 
									sqlDataSource.getChannelsFlag(), sqlDataSource.getTranslationsFlag(), 
									sqlDataSource.getRanksFlag(), sqlDataSource.getColumnsFlag());
							} else {
								sqlDataSource.defaultCreateChannel(channel, tid, 
									sqlDataSource.getChannelsFlag(), sqlDataSource.getTranslationsFlag(), 
									sqlDataSource.getRanksFlag(), sqlDataSource.getColumnsFlag());
							}
						}
					}
				}
				
				// store the reference to the initialized data source in the map of initialized data sources
				sqlDataSourceMap.put(dataSource, sqlDataSource);
			}
		}
	}
	
	/**
	 * Parse file from url (resource locator or file name)
	 * @param filename
	 */
	public void process(String filename) {
		
		try {
			
			// define the channel code for this instance
			channelCode	= channel.getCode();
			
			// define the settings for this instance
			settings	= settingMap.get(channelCode);
			
			// default the connection to the station
			connection	= new LilyIPConnection(deviceIP, devicePort);
			
			// get the latest data time from the tilt database
			Date lastDataTime = sqlDataSource.defaultGetLastDataTime(channel.getCode());
			if (lastDataTime == null) {
				lastDataTime = new Date(0);
			}
			
			String line			= "";
			int lineNumber		= 1;			
			boolean reconnect	= true;
			
			logger.log(Level.INFO, "Begin Streaming " + channelCode + "] (lastDataTime: " + dateOut.format(lastDataTime) + ")");
			
			while (true) {
				
				// establish a connection to the station
				if (reconnect) {
					try {
						connection.open();
						Thread.sleep(postConnectDelay);
						reconnect = false;
					} catch (Exception e) {
						reconnect = true;
						e.printStackTrace();	
					}	
				}
				
				// once connected, begin receiving messages
				if (!reconnect) {
					
					// get the message from the device
					line = connection.getMsg(settings.dataTimeout);
					
					// if the message is empty, give up on this try, and try again
					if (line == null) {
						logger.log(Level.INFO, channelCode + " message was null at " + currentTime.nowString());
						continue;
					}
					
					try {
						
						// remove the $ and new line character from the message (lily specific)
						line	= line.substring(1, line.length() - 1).trim();
						
						// split the data row into an ordered list. be sure to use the two argument split, as some lines may have many trailing delimiters
						String[] valueArray					= line.split(settings.delimiter, -1);
						HashMap<Integer, String> valueMap	= new HashMap<Integer, String>();
						for (int i = 0; i < valueArray.length; i++) {
							valueMap.put(i, valueArray[i].trim());
						}
						
						// make sure the data row matches the defined data columns
						if (importColumnMap.size() != valueMap.size()) {
							if (importColumnMap.size() > valueMap.size()) {
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
						HashMap<Integer, String> dsColumnMap;
						for (int i = 0; i < dataSourceList.size(); i++) {
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
								channel = sqlDataSource.defaultGetChannel(channelCode, sqlDataSource.getChannelTypesFlag());
								if (channel == null) {
									channel = new Channel(0, channelCode, null, Double.NaN, Double.NaN, Double.NaN);
									sqlDataSource.defaultCreateChannel(channel, 1,
											sqlDataSource.getChannelsFlag(), sqlDataSource.getTranslationsFlag(), 
											sqlDataSource.getRanksFlag(), sqlDataSource.getColumnsFlag());
									channel = sqlDataSource.defaultGetChannel(channelCode, sqlDataSource.getChannelTypesFlag());
								}
							}
							
							// columns for this data source
							columns			= dataSourceColumnMap.get(dataSource);
							columnArray		= columns.split(",");
							dsColumnMap		= new HashMap<Integer, String>();
							for (int j = 0; j < columnArray.length; j++) {
								dsColumnMap.put(j, columnArray[j]);
							}
							
							// rank for this data source.  this should already exist in the database
							if (sqlDataSource.getRanksFlag()) {
								rank	= sqlDataSource.defaultGetRank(rank.getId());
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
							
							// assign the double matrix and column names to a generic data matrix
							GenericDataMatrix gdm = new GenericDataMatrix(dm);
							gdm.setColumnNames(columnNames);
							
							// insert the data to the database
							logger.log(Level.INFO, channel.getCode() + "[" + lineNumber + "] (" + dataSource + ") " + lineOutput);
							sqlDataSource.insertData(channel.getCode(), gdm, sqlDataSource.getTranslationsFlag(), sqlDataSource.getRanksFlag(), rank.getId());
							
							// VALVE2 HACK
							double j2ksec, xTilt, yTilt, holeTemp, boxTemp, instVolt, gndVolt, rainfall;
							DoubleMatrix2D temp;
							if (dataSource.equals("hvo_deformation") && sqlDataSource.getType().equals("tilt")) {
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
							}
						}
						
						line	= rr.nextLine();
						lineNumber++;
						
					} catch (Exception e) {
						connection.close();
						reconnect = true;
					}					
				}	
				// Thread.sleep(betweenPollDelay);
			}
			
		} catch (Exception e) {
			logger.log(Level.SEVERE, "DataStreamer.process() failed.", e);
		}
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
		
		ImportStream importer	= new ImportStream();
		
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
		
		importer.process("");
		
		System.exit(0);
	}	
}
