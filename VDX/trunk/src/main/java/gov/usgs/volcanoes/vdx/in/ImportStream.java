package gov.usgs.volcanoes.vdx.in;

import java.lang.reflect.Constructor;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import gov.usgs.plot.data.GenericDataMatrix;
import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.volcanoes.vdx.data.Channel;
import gov.usgs.volcanoes.vdx.data.Column;
import gov.usgs.volcanoes.vdx.data.Rank;
import gov.usgs.volcanoes.vdx.data.SQLDataSource;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceHandler;
import gov.usgs.volcanoes.vdx.in.conn.Connection;
import gov.usgs.volcanoes.vdx.in.hw.Device;
import cern.colt.matrix.*;


/**
 * A program to stream data from an ip device and put it in the database
 *
 * @author Loren Antolik
 */
public class ImportStream extends Import implements Importer {

	public ConfigFile stationParams;
	public ConfigFile deviceParams;
	public ConfigFile connectionParams;
	
	public String stationCode;
	public List<String> stationList;
	public Map<String, String> stationChannelMap;
	public Map<String, Device> stationDeviceMap;
	public Map<String, Connection> stationConnectionMap;
	public Map<String, ConfigFile> stationConnectionParamsMap;
	public Map<String, String> stationTimesourceMap;
	
	public String timesource;
	public boolean lastDataTimeNow;

	public int postConnectDelay;
	public int betweenPollDelay;
	public int betweenCycleDelay;
	
	public Connection connection;	
	public Device device;

	/**
	 * takes a config file as a parameter and parses it to prepare for importing
	 * @param cf configuration file
	 * @param verbose true for info, false for severe
	 */
	public void initialize(String importerClass, String configFile, boolean verbose) {
		
		// initialize the logger for this importer
		logger	= Logger.getLogger(importerClass);
		
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
		
		// define a format for log message dates
		dateOut	= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));

		// get connection settings related to this instance
		postConnectDelay	= Util.stringToInt(params.getString("postConnectDelay"), 1000);	
		betweenPollDelay	= Util.stringToInt(params.getString("betweenPollDelay"), 1000);	
		betweenCycleDelay	= Util.stringToInt(params.getString("betweenCycleDelay"), 1000);	
		
		// get the rank configuration for this import.  there can only be a single rank per import
		rankParams		= params.getSubConfig("rank");
		rankName		= Util.stringToString(rankParams.getString("name"), "Raw Data");
		rankValue		= Util.stringToInt(rankParams.getString("value"), 1);
		rankDefault		= Util.stringToInt(rankParams.getString("default"), 0);
		rank			= new Rank(0, rankName, rankValue, rankDefault);
		logger.log(Level.INFO, "[Rank] " + rankName);
		logger.log(Level.INFO, "");
		
		// get the channel configurations for this import.  there can be multiple channels per import
		channelMap		= new HashMap<String, Channel>();
		stringList		= params.getList("channel");
		for (int i = 0; i < stringList.size(); i++) {
			channelCode		= stringList.get(i);
			channelParams	= params.getSubConfig(channelCode);
			channelName		= Util.stringToString(channelParams.getString("name"), channelCode);
			channelLon		= Util.stringToDouble(channelParams.getString("longitude"), Double.NaN);
			channelLat		= Util.stringToDouble(channelParams.getString("latitude"), Double.NaN);
			channelHeight	= Util.stringToDouble(channelParams.getString("height"), Double.NaN);
			channelActive	= Util.stringToInt(channelParams.getString("active"), 1);
			channel			= new Channel(0, channelCode, channelName, channelLon, channelLat, channelHeight, channelActive);
			channelMap.put(channelCode, channel);
		}
		
		// define the station objects to store station configurations
		stationList					= new ArrayList<String>();
		channelList					= new ArrayList<String>();
		stationChannelMap			= new HashMap<String, String>();
		stationDeviceMap			= new HashMap<String, Device>();
		stationConnectionParamsMap	= new HashMap<String, ConfigFile>();
		stationTimesourceMap		= new HashMap<String, String>();
		
		// validate that station are defined in the config file
		stringList	= params.getList("station");
		if (stringList == null) {
			logger.log(Level.SEVERE, "station parameter(s) missing from config file");
			System.exit(-1);			
		}
		
		// get the list of station that are being used in this import
		for (int i = 0; i < stringList.size(); i++) {
			
			// station configuration
			stationCode			= stringList.get(i);
			stationParams		= params.getSubConfig(stationCode);
			deviceParams		= stationParams.getSubConfig("device");
			connectionParams	= stationParams.getSubConfig("connection");
			channelCode			= Util.stringToString(stationParams.getString("channel"), stationCode);
			timesource			= stationParams.getString("timesource");
			
			// verify that the time source is configured for this station
			if (timesource == null) {
				logger.log(Level.SEVERE, "timesource parameter for " + stationCode + " missing from config file");
				System.exit(-1);			
			}

			// try to create a connection object
			try {
				Class<?> connClass	= Class.forName(connectionParams.getString("driver"));				
				Constructor<?> cnst	= connClass.getConstructor(new Class[]{String.class});
				connection			= (Connection)cnst.newInstance(new Object[]{stationCode});
				connection.initialize(connectionParams);
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Connection initialization failed", e);
				continue;
			}
			
			// try to create a device object
			try {
				device = (Device)Class.forName(deviceParams.getString("driver")).newInstance();
				device.initialize(deviceParams);
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Device driver initialization failed", e);
				System.exit(-1);
			}
			
			stationList.add(stationCode);
			channelList.add(channelCode);
			stationChannelMap.put(stationCode, channelCode);
			stationDeviceMap.put(stationCode, device);
			stationConnectionParamsMap.put(stationCode, connectionParams);
			stationTimesourceMap.put(stationCode, timesource);
			
			// display configuration information related to this station
			logger.log(Level.INFO, "[Station] " + stationCode);
			logger.log(Level.INFO, "[Connection] " + connection.toString());
			logger.log(Level.INFO, "[ConnDriver] " + connectionParams.getString("driver"));
			logger.log(Level.INFO, "[Device] " + device.toString());
			logger.log(Level.INFO, "[DevDriver] " + deviceParams.getString("driver"));
			logger.log(Level.INFO, "[Fields] " + device.getFields());
			logger.log(Level.INFO, "[Channel] " + channelCode);
			logger.log(Level.INFO, "");
			
			// destroy this temporary connection
			connection = null;
		}
		
		// define a comma separated list of channels affected in this import
		channelList	= new ArrayList<String>(new HashSet<String>(channelList));
		defaultChannels	= "";
		for (int i = 0; i < channelList.size(); i++) {
			defaultChannels += channelList.get(i) + ",";
		}
		defaultChannels	= defaultChannels.substring(0, defaultChannels.length() - 1);
		logger.log(Level.INFO, "[defaultChannels] " + defaultChannels);
		
		// validate that data sources are defined in the config file
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
		dataSourceRIDMap		= new HashMap<String, Integer>();
		
		// iterate through each of the data sources and setup the db for it
		for (int i = 0; i < dataSourceList.size(); i++) {
			
			// get the data source name
			dataSource	= dataSourceList.get(i);
			logger.log(Level.INFO, "[DataSource] " + dataSource);
			
			// lookup the data source from the list that is in vdxSources.config
			sqlDataSourceDescriptor	= sqlDataSourceHandler.getDataSourceDescriptor(dataSource);
			if (sqlDataSourceDescriptor == null) {
				logger.log(Level.SEVERE, dataSource + " not in vdxSources.config - Skipping");
				continue;
			}
			
			// formally get the data source from the list of descriptors.  this will initialize the data source which includes db creation
			sqlDataSource	= sqlDataSourceDescriptor.getSQLDataSource();
			
			// store the reference to the initialized data source in the map of initialized data sources
			sqlDataSourceMap.put(dataSource, sqlDataSource);
			
			// get the config for this data source
			dataSourceParams	= params.getSubConfig(dataSource);	
			
			// if this is a ranked data source, then create the rank in the database
			if (sqlDataSource.getRanksFlag()) {
				Rank tempRank	= sqlDataSource.defaultGetRank(rank);
				if (tempRank == null) {
					tempRank = sqlDataSource.defaultInsertRank(rank);
				}
				if (tempRank == null) {
					logger.log(Level.SEVERE, dataSource + " " + rank.getName() + " rank creation failed");
					System.exit(-1);
				}
				dataSourceRIDMap.put(dataSource, tempRank.getId());
			}
			
			// columns based configuration
			if (sqlDataSource.getColumnsFlag()) {

				// look up columns from the config file and try to insert them into the database
				stringList	= dataSourceParams.getList("column");
				if (stringList != null) {
					for (int j = 0; j < stringList.size(); j++) {
						columnName			= stringList.get(j);
						columnParams		= dataSourceParams.getSubConfig(columnName);
						columnIdx			= Util.stringToInt(columnParams.getString("idx"), i);
						columnDescription	= Util.stringToString(columnParams.getString("description"), columnName);
						columnUnit			= Util.stringToString(columnParams.getString("unit"), columnName);
						columnChecked		= Util.stringToBoolean(columnParams.getString("checked"), false);
						columnActive		= Util.stringToBoolean(columnParams.getString("active"), true);
						columnBypass		= Util.stringToBoolean(columnParams.getString("bypass"), false);
						columnAccumulate	= Util.stringToBoolean(columnParams.getString("accumulate"), false);
						column 				= new Column(columnIdx, columnName, columnDescription, 
								columnUnit, columnChecked, columnActive, columnBypass, columnAccumulate);
						if (sqlDataSource.defaultGetColumn(columnName) == null) {
							sqlDataSource.defaultInsertColumn(column);
						}
					}
				}
			
				// generate a list of all the columns in the database for this data source
				columns		= "";
				columnList	= sqlDataSource.defaultGetColumns(true, false);
				for (int j = 0; j < columnList.size(); j++) {
					columns	   += columnList.get(j).name + ",";
				}
				columns		= columns.substring(0, columns.length() - 1);
				columns	= Util.stringToString(dataSourceParams.getString("columns"), columns);
				dataSourceColumnMap.put(dataSource, columns);
				logger.log(Level.INFO, "[Columns] " + columns);
			}
			
			// create translations table which is based on column entries
			if (sqlDataSource.getTranslationsFlag()) {
				sqlDataSource.defaultCreateTranslation();
			}
			
			// get the channels for this data source
			channels = Util.stringToString(dataSourceParams.getString("channels"), defaultChannels);
			dataSourceChannelMap.put(dataSource, channels);
			logger.log(Level.INFO, "[Channels]" + channels);

			// create channels tables for this data source
			if (sqlDataSource.getChannelsFlag()) {				
				channelArray = channels.split(",");				
				for (int j = 0; j < channelArray.length; j++) {
					channelCode		= channelArray[j];
					channel 		= channelMap.get(channelCode);
					channelParams	= params.getSubConfig(channelCode);
					
					if (channel == null) {
						continue;
					}
					
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
						channelMap.put(channelCode, channel);
					}
					
					// create a new translation if any non-default values were specified, and use the new tid for the create channel statement
					if (sqlDataSource.getTranslationsFlag()) {
						int tid			= 1;
						int extraColumn	= 0;
						double multiplier, offset;
					
						// get the translations sub config for this channel
						translationParams	= channelParams.getSubConfig("translation"); 
						
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
			
		// output initial polling message
		logger.log(Level.INFO, "");
		logger.log(Level.INFO, "BEGIN STREAMING CYCLE");
		
		// add an extra message to notify the nature of a streaming process
		if (stationList.size() != 1) {
			logger.log(Level.SEVERE, "ImportStream supports only one station");
			System.exit(-1);
		}
		
		// get the station name, and it's associated configuration
		stationCode			= stationList.get(0);
		channelCode			= stationChannelMap.get(stationCode);
		device				= stationDeviceMap.get(stationCode);
		connectionParams	= stationConnectionParamsMap.get(stationCode);
		timesource			= stationTimesourceMap.get(stationCode);
		
		// get the import line definition for this channel
		fieldArray	= device.getFields().split(",");
		fieldMap	= new HashMap<Integer, String>();
		for (int i = 0; i < fieldArray.length; i++) {
			fieldMap.put(i, fieldArray[i].trim());
		}
		
		// get the latest data time from data source that keeps track of time
		sqlDataSource		= sqlDataSourceMap.get(timesource);
		Date lastDataTime	= sqlDataSource.defaultGetLastDataTime(channelCode, device.getNullfield(), device.getPollhist());
		
		// display logging information
		logger.log(Level.INFO, "");
		logger.log(Level.INFO, "Streaming " + stationCode + " [lastDataTime:" + dateOut.format(lastDataTime) + "]");
		
		// initialize data objects related to this device
		dateIn	= new SimpleDateFormat(device.getTimestamp());
		dateIn.setTimeZone(TimeZone.getTimeZone(device.getTimezone()));
		
		// create a connection to the station
		try {
			Class<?> connClass	= Class.forName(connectionParams.getString("driver"));				
			Constructor<?> cnst	= connClass.getConstructor(new Class[]{String.class});
			connection			= (Connection)cnst.newInstance(new Object[]{stationCode});
			connection.initialize(connectionParams);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Connection initialization failed", e);
			System.exit(-1);
		}

		// initialize the reconnect flag to force connect the first time
		boolean reconnect	= true;			
		String line			= "";
		int lineNumber		= 0;
		
		// continue trying to acquire data until the program exits
		while (true) {	
			
			// connect to the device
			if (reconnect) {
				try {
					connection.connect();
					reconnect = false;
					Thread.sleep(postConnectDelay);
				} catch (Exception e) {
					logger.log(Level.SEVERE, "Station Connection failed", e);
					if (connection.isOpen()) connection.disconnect();
					reconnect = true;
					continue;
				}
			}
			
			// try to build the data request string
			String dataRequest = "";
			try {
				dataRequest	= device.requestData(lastDataTime);
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Device build request failed", e);
				continue;
			}
			
			// send the request to the device
			if (dataRequest.length() > 0) {
				try {
					connection.writeString(dataRequest);
				} catch (Exception e) {
					logger.log(Level.SEVERE, "Connection send data request failed", e);
					continue;
				}
			}
			
			// try wait (eh) for the response from the device (clear out the message queue first)
			String dataResponse = "";
			try {
				dataResponse	= connection.readString(device);
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Device receive data response failed", e);
				continue;
			}
			
			// try to validate the response from the device
			try {
				device.validateMessage(dataResponse, true);
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Message validation failed", e);
				continue;
			}
				
			// format the response based on the type of device
			String dataMessage	= device.formatMessage(dataResponse);
				
			// parse the response by lines
			StringTokenizer st	= new StringTokenizer(dataMessage, "\n");
				
			// iterate through each line
			while (st.hasMoreTokens()) {
				
				// increment the line number variable
				lineNumber++;
				
				// save this token for processing
				line = st.nextToken();
					
				// try to validate this data line
				try {
					device.validateLine(line);
				} catch (Exception e) {
					logger.log(Level.INFO, "invalid:" + line);
					continue;
				}
				
				// format this data line
				line = device.formatLine(line);
				
				// output this line to the log file
				logger.log(Level.INFO, line);
				
				// split the data row into an ordered list. be sure to use the two argument split, as some lines may have many trailing delimiters
				Pattern p	= Pattern.compile(device.getDelimiter());
				String[] valueArray		= p.split(line, -1);
				HashMap<Integer, String> valueMap	= new HashMap<Integer, String>();
				for (int i = 0; i < valueArray.length; i++) {
					valueMap.put(i, valueArray[i].trim());
				}
				
				// make sure the data row matches the defined data columns
				if (fieldMap.size() > valueMap.size()) {
					logger.log(Level.SEVERE, "line " + lineNumber + " has too few values:" + line);
					continue;
				}
				
				// map the columns to the values.  look for the TIMESTAMP and CHANNEL flags
				HashMap<Integer, ColumnValue> columnValueMap = new HashMap<Integer, ColumnValue>();
				ColumnValue columnValue;
				String name;
				double value;
				int count		= 0;
				String tsValue	= "";
				
				// try to parse the values from this data line
				try {
					for (int i = 0; i < fieldMap.size(); i++) {								
						name		= fieldMap.get(i);
						
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
					
				// any problems with parsing the values for this line should be caught here
				} catch (Exception e) {
					logger.log(Level.SEVERE, "line " + lineNumber + " parse error");
					logger.log(Level.SEVERE, e.getMessage());
					continue;
				}
				
				// make sure that the timestamp has something in it
				if (tsValue.length() == 0) {
					logger.log(Level.SEVERE, "line " + lineNumber + " timestamp not found");
					continue;
				}
				
				// convert the timezone of the input date and convert to j2ksec
				try {
					String timestamp	= tsValue.trim();
					date				= dateIn.parse(timestamp);				
					j2ksec				= Util.dateToJ2K(date);
				} catch (ParseException e) {
					logger.log(Level.SEVERE, "line " + lineNumber + " timestamp parse error");
					continue;
				}
					
				ColumnValue tsColumn = new ColumnValue("j2ksec", j2ksec);
					
				// iterate through each data source that was defined and assign data from this line to it
				for (int i = 0; i < dataSourceList.size(); i++) {
					
					// get the data source name and it's associated sql data source
					dataSource		= dataSourceList.get(i);
					channels		= dataSourceChannelMap.get(dataSource);
					dsChannelArray	= channels.split(",");				
					
					// lookup in the channels map to see if we are filtering on stations
					boolean channelMemberOfDataSource = false;
					for (int j = 0; j < dsChannelArray.length; j++) {
						if (dsChannelArray[j].equals(channelCode)) {
							channelMemberOfDataSource = true;
							continue;
						}
					}						
					if (!channelMemberOfDataSource) {
						continue;
					}
					
					// check that the sql data source was initialized properly above
					sqlDataSource	= sqlDataSourceMap.get(dataSource);
					if (sqlDataSource == null) {
						logger.log(Level.SEVERE, "line " + lineNumber + " data source " + dataSource + " not initialized");
						continue;
					}
					
					// channel for this data source.  create it if it doesn't exist
					if (sqlDataSource.getChannelsFlag()) {
						if (sqlDataSource.defaultGetChannel(channelCode, sqlDataSource.getChannelTypesFlag()) == null) {
							sqlDataSource.defaultCreateChannel(new Channel(0, channelCode, channelCode, Double.NaN, Double.NaN, Double.NaN, 1), 1,
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
						rid	= dataSourceRIDMap.get(dataSource);
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
			}
		}
	}
	
	public void outputInstructions(String importerClass, String message) {
		if (message != null) {
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
