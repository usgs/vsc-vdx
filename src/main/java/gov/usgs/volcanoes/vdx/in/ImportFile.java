package gov.usgs.volcanoes.vdx.in;

import gov.usgs.volcanoes.core.time.J2kSec;
import gov.usgs.volcanoes.core.util.StringUtils;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;

import gov.usgs.volcanoes.core.data.GenericDataMatrix;
import gov.usgs.volcanoes.core.legacy.Arguments;
import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.util.ResourceReader;
import gov.usgs.volcanoes.vdx.data.Channel;
import gov.usgs.volcanoes.vdx.data.Column;
import gov.usgs.volcanoes.vdx.data.Rank;
import gov.usgs.volcanoes.vdx.data.SQLDataSource;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceHandler;

import cern.colt.matrix.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Import files
 *  
 * @author Loren Antolik
 * @author Bill Tollett
 */
public class ImportFile extends Import implements Importer {

	private static final Logger LOGGER = LoggerFactory.getLogger(ImportFile.class);
	public ResourceReader rr;

	/**
	 * takes a config file as a parameter and parses it to prepare for importing
	 * @param configFile configuration file
	 * @param verbose true for info, false for severe
	 */
	public void initialize(String importerClass, String configFile, boolean verbose) {
		
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
		
		LOGGER.info("Reading config file {}", configFile);
		
		// initialize the config file and verify that it was read
		params		= new ConfigFile(configFile);
		if (!params.wasSuccessfullyRead()) {
			LOGGER.error("{} was not successfully read", configFile);
			System.exit(-1);
		}
		
		// get the vdx parameter, and exit if it's missing
		vdxConfig	= StringUtils.stringToString(params.getString("vdx.config"), "VDX.config");
		if (vdxConfig == null) {
			LOGGER.error("vdx.config parameter missing from config file");
			System.exit(-1);
		}
		
		// get the vdx config as it's own config file object
		vdxParams	= new ConfigFile(vdxConfig);
		driver		= vdxParams.getString("vdx.driver");
		url			= vdxParams.getString("vdx.url");
		prefix		= vdxParams.getString("vdx.prefix");
		
		// information related to the time stamps
		dateIn	= new SimpleDateFormat(StringUtils.stringToString(params.getString("timestamp"), "yyyy-MM-dd HH:mm:ss"));
		dateIn.setTimeZone(TimeZone.getTimeZone(StringUtils.stringToString(params.getString("timezone"), "GMT")));
		
		// ImportFile specific directives
		filemask	= StringUtils.stringToString(params.getString("filemask"), "");
		headerlines	= StringUtils.stringToInt(params.getString("headerlines"), 0);
		delimiter	= StringUtils.stringToString(params.getString("delimiter"), ",");
		LOGGER.info("filemask:{}/headerlines:{}/delimiter:{}", filemask, headerlines, delimiter);
		
		// Import Fields
		fields	= StringUtils.stringToString(params.getString("fields"), "");
		if (fields.length() == 0) {
			LOGGER.error("fields parameter missing from config file");
			System.exit(-1);
		}
		
		// create a default field map
		fieldArray		= fields.split(",");
		defaultFieldMap	= new HashMap<Integer, String>();
		for (int i = 0; i < fieldArray.length; i++) {
			defaultFieldMap.put(i, fieldArray[i].trim());
		}
		
		// get the list of ranks that are being used in this import
		rankParams		= params.getSubConfig("rank");
		rankName		= StringUtils.stringToString(rankParams.getString("name"), "Raw Data");
		rankValue		= StringUtils.stringToInt(rankParams.getString("value"), 1);
		rankDefault		= StringUtils.stringToInt(rankParams.getString("default"), 0);
		rank			= new Rank(0, rankName, rankValue, rankDefault);
		LOGGER.info("[Rank] {}", rankName);
		LOGGER.info("");
		
		// get the channel configurations for this import.  there can be multiple channels per import
		channelMap		= new HashMap<String, Channel>();
		channelFieldMap	= new HashMap<String, String>();
		stringList		= params.getList("channel");
		if (stringList != null) {
			for (int i = 0; i < stringList.size(); i++) {
				channelCode		= stringList.get(i);
				channelParams	= params.getSubConfig(channelCode);
				channelName		= StringUtils.stringToString(channelParams.getString("name"), channelCode);
				channelLon		= StringUtils.stringToDouble(channelParams.getString("longitude"), Double.NaN);
				channelLat		= StringUtils.stringToDouble(channelParams.getString("latitude"), Double.NaN);
				channelHeight	= StringUtils.stringToDouble(channelParams.getString("height"), Double.NaN);
				channelActive   = StringUtils.stringToInt(channelParams.getString("active"), 1);
				channel			= new Channel(0, channelCode, channelName, channelLon, channelLat, channelHeight, channelActive);
				channelMap.put(channelCode, channel);
				
				// default the fields for this channel if they are not specified specifically
				channelFieldMap.put(channelCode, StringUtils.stringToString(channelParams.getString("fields"), fields));
			}
		}
		
		// get the list of data sources that are being used in this import
		dataSourceList	= params.getList("dataSource");
		if (dataSourceList == null) {
			LOGGER.error("dataSource parameter(s) missing from config file");
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
			LOGGER.info("[DataSource] {}", dataSource);
			
			// lookup the data source from the list that is in vdxSources.config
			sqlDataSourceDescriptor	= sqlDataSourceHandler.getDataSourceDescriptor(dataSource);
			if (sqlDataSourceDescriptor == null) {
				LOGGER.error("{} not in vdxSources.config - Skipping", dataSource);
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
					LOGGER.error("{} {} rank creation failed.", dataSource, rank.getName());
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
						columnIdx			= StringUtils.stringToInt(columnParams.getString("idx"), i);
						columnDescription	= StringUtils.stringToString(columnParams.getString("description"), columnName);
						columnUnit			= StringUtils.stringToString(columnParams.getString("unit"), columnName);
						columnChecked		= StringUtils.stringToBoolean(columnParams.getString("checked"), false);
						columnActive		= StringUtils.stringToBoolean(columnParams.getString("active"), true);
						columnBypass		= StringUtils.stringToBoolean(columnParams.getString("bypass"), false);
						columnAccumulate	= StringUtils.stringToBoolean(columnParams.getString("accumulate"), false);
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
				columns	= columns.substring(0, columns.length() - 1);
				columns	= StringUtils.stringToString(dataSourceParams.getString("columns"), columns);
				dataSourceColumnMap.put(dataSource, columns);
				LOGGER.info("[Columns] {}", columns);
			}
			
			// create translations table which is based on column entries
			if (sqlDataSource.getTranslationsFlag()) {
				sqlDataSource.defaultCreateTranslation();
			}
			
			// get the channels for this data source
			channels = StringUtils.stringToString(dataSourceParams.getString("channels"), "");
			dataSourceChannelMap.put(dataSource, channels);
			LOGGER.info("[Channels] {}", channels);

			// create channels tables for this data source
			if (sqlDataSource.getChannelsFlag() && channels.length() > 0) {				
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
							azimuthNom	= StringUtils.stringToDouble(channelParams.getString("azimuth"), 0);
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
							azimuthInst	= StringUtils.stringToDouble(translationParams.getString("azimuth"), 0);
							dm.setQuick(0, columnList.size() * 2, azimuthInst);
							columnNames[columnList.size() * 2]	= "azimuth";
						}
						
						// iterate through the column list to get the translation values
						for (int k = 0; k < columnList.size(); k++) {
							column		= columnList.get(k);
							columnName	= column.name;
							multiplier	= StringUtils.stringToDouble(translationParams.getString("c" + columnName), 1);
							offset		= StringUtils.stringToDouble(translationParams.getString("d" + columnName), 0);
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
						if (tid != sqlDataSource.defaultGetChannelTranslationId(channel.getCode())) {
							sqlDataSource.defaultUpdateChannelTranslationId(channel.getCode(), tid);
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
			
			// instantiate variables used by this method
			HashMap<Integer, ColumnValue> columnValueMap = new HashMap<Integer, ColumnValue>();
			ColumnValue columnValue;
			String name, line, tsValue;
			int count, lineNumber;
			double value;
			boolean channelCodeFromFilename = false;
			
			// check that the file exists
			ResourceReader rr = ResourceReader.getResourceReader(filename);
			if (rr == null) {
				LOGGER.error("skipping: {} (resource is invalid)", filename);
				return;
			}
			
			// make a short file name.  we'll use this later on
			String shortFilename = filename.substring(filename.lastIndexOf("/") + 1);
			
			// move to the first line in the file
			line		= rr.nextLine();
			lineNumber	= 0;
			
			// check that the file has data
			if (line == null) {
				LOGGER.error("skipping: {} (resource is empty)", filename);
				return;
			}
			
			LOGGER.info("");
			LOGGER.info("importing: {}", filename);
			
			// reset the channel code, as it will be derived from the filename, and not the config file, or the contents of the file
			channelCode	= "";
			fieldMap	= defaultFieldMap;
			
			// if a filename mask is defined, then get the channel code from it
			if (filemask.length() > 0) {
				
				// filename mask can be shorter than the filename, but not longer
				if (filemask.length() > shortFilename.length()) {
					LOGGER.error("skipping: {} (bad filename mask)", filename);
					return;
				}
				
				// build up the channel code from the mask
				for (int i = 0; i < filemask.length(); i++) {
					if (String.valueOf(filemask.charAt(i)).equals("C")) {
						channelCode	= channelCode + String.valueOf(shortFilename.charAt(i));
					}
				}
				
				// lookup custom fields for this channel if they exist
				if (channelCode.length() == 0) {
					LOGGER.error("skipping: {} (filename does not contain channel code)", filename);
					return;
				}
				
				// LOGGER.log(Level.INFO, "channelCode:" + channelCode + " (from filename)");
				
				// indicate the channel code came from the file name and look up it's fields if they exist
				channelCodeFromFilename	= true;
				channelFields			= channelFieldMap.get(channelCode);
				if (channelFields != null) {
					fieldArray	= channelFields.split(",");
					fieldMap	= new HashMap<Integer, String>();
					for (int i = 0; i < fieldArray.length; i++) {
						fieldMap.put(i, fieldArray[i].trim());
					}
				}
			}
			
			// if any header lines are defined then skip them
			if (headerlines > 0) {
				LOGGER.info("skipping {} header lines", headerlines);
				for (int i = 0; i < headerlines; i++) {
					line	= rr.nextLine();	
					lineNumber++;
				}
			}
			
			// we are now at the first row of data.  time to import!
			while (line != null) {
				
				// increment the line number variable
				lineNumber++;
				
				// split the data row into an ordered list. be sure to use the two argument split, as some lines may have many trailing delimiters
				Pattern p	= Pattern.compile(delimiter);
				String[] valueArray	= p.split(line, -1);
				HashMap<Integer, String> valueMap	= new HashMap<Integer, String>();
				for (int i = 0; i < valueArray.length; i++) {
					valueMap.put(i, valueArray[i].replaceAll("[\'\"]", "").trim());
				}
				
				// make sure the data row matches the defined data columns
				if (fieldMap.size() > valueMap.size()) {
					LOGGER.error("line {} has too few values", lineNumber);
					line	= rr.nextLine();
					continue;
				}
				
				// if the channel code has not been defined in the filename, then it is in the line
				if (!channelCodeFromFilename) {

					// default the channel code to empty
					channelCode	= "";
					
					// look up the channel code in the line
					for (int i = 0; i < fieldMap.size(); i++) {
						name	= fieldMap.get(i);
						if (name.equals("CHANNEL")) {
							channelCode	= valueMap.get(i);
							break;
						}
					}
					
					// validate the channel code 
					if (channelCode.length() == 0) {
						LOGGER.error("line {} does not contain a channel code", lineNumber);
						line	= rr.nextLine();
						continue;
						
					} else {
					
						// look up the field definition for this channel code
						channelFields	= channelFieldMap.get(channelCode);
						if (channelFields != null) {
							fieldArray	= channelFields.split(",");
							fieldMap	= new HashMap<Integer, String>();
							for (int i = 0; i < fieldArray.length; i++) {
								fieldMap.put(i, fieldArray[i].trim());
							}
						} else {
							fieldMap	= defaultFieldMap;
						}
					}
				}
				
				// try to parse the values from this data line
				count	= 0;
				tsValue	= "";
				try {
					for (int i = 0; i < fieldMap.size(); i++) {					
						name	= fieldMap.get(i);
						
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
							if (valueMap.get(i).length() == 0 || valueMap.get(i).equalsIgnoreCase("NAN")) {
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
					LOGGER.error("line {} parse error", lineNumber);
					LOGGER.error("{}", e.getMessage());
					line	= rr.nextLine();
					continue;
				}
				
				// make sure that the channel code has something in it
				if (channelCode.length() == 0) {
					LOGGER.error("line {} channel code not found", lineNumber);
					line	= rr.nextLine();
					continue;
					
				// convert bad sql characters to dollar signs
				} else {
					channelCode = channelCode.replace('\\', '$').replace('/', '$').replace('.', '$').replace(' ', '$');;
				}
				
				// make sure that the timestamp has something in it
				if (tsValue.length() == 0) {
					LOGGER.error("line {} timestamp not found", lineNumber);
					line	= rr.nextLine();
					continue;
				}
				
				// convert the time zone of the input date and convert to j2ksec
				try {
					String timestamp	= tsValue.trim();
					date				= dateIn.parse(timestamp);				
					j2ksec				= J2kSec.fromDate(date);
				} catch (ParseException e) {
					LOGGER.error("line {} timestamp parse error", lineNumber);
					line	= rr.nextLine();
					continue;
				}
				
				// log the line to the log file that is being imported, now that all potential errors have been caught
				// LOGGER.log(Level.INFO, line);
				
				ColumnValue tsColumn = new ColumnValue("j2ksec", j2ksec);
				
				// iterate through each data source that was defined and assign data from this line to it
				for (int i = 0; i < dataSourceList.size(); i++) {
					
					// get the data source name and it's associated sql data source
					dataSource		= dataSourceList.get(i);
					channels		= dataSourceChannelMap.get(dataSource);
					
					// lookup in the channels map to see if we are filtering on stations
					boolean channelMemberOfDataSource = false;
					if (channels.length() > 0) {
						dsChannelArray	= channels.split(",");				
						for (int j = 0; j < dsChannelArray.length; j++) {
							if (dsChannelArray[j].equals(channelCode)) {
								channelMemberOfDataSource = true;
								continue;
							}
						}						
						if (!channelMemberOfDataSource) {
							continue;
						}
					}
					
					// check that the sql data source was initialized properly above
					sqlDataSource	= sqlDataSourceMap.get(dataSource);
					if (sqlDataSource == null) {
						LOGGER.error("line {} data source {} not initialized", lineNumber, dataSource);
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
				
				// go to the next line
				line	= rr.nextLine();
			}
			
			// close the resource
			rr.close();
		
		// catch exceptions
		} catch (Exception e) {
			LOGGER.error("ImportFile.process({}) failed.", filename, e);
		}
	}
	
	public void outputInstructions(String importerClass, String message) {
		if (message != null) {
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

