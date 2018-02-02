package gov.usgs.volcanoes.vdx.in;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

import gov.usgs.volcanoes.core.data.GenericDataMatrix;
import gov.usgs.volcanoes.core.legacy.Arguments;
import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.time.J2kSec;
import gov.usgs.volcanoes.core.util.StringUtils;
import gov.usgs.volcanoes.vdx.data.Channel;
import gov.usgs.volcanoes.vdx.data.Column;
import gov.usgs.volcanoes.vdx.data.Rank;
import gov.usgs.volcanoes.vdx.data.SQLDataSource;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceHandler;
import gov.usgs.volcanoes.vdx.in.conn.Connection;
import gov.usgs.volcanoes.vdx.in.hw.Device;
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
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A program to poll data from a collection of ip devices and put it in the database.
 * 
 * @author Loren Antolik
 * @author Bill Tollett
 */
public class ImportPoll extends Import implements Importer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImportPoll.class);

  public ConfigFile stationParams;
  public ConfigFile deviceParams;
  public ConfigFile connectionParams;

  public String stationCode;
  public List<String> stationList;
  public Map<String, String> stationChannelMap;
  public Map<String, Device> stationDeviceMap;
  public Map<String, ConfigFile> stationConnectionParamsMap;
  public Map<String, String> stationTimesourceMap;
  public Map<String, Date> stationLastDataTimeMap;

  public String timesource;
  public Date lastDataTime;

  public int postConnectDelay;
  public int betweenPollDelay;
  public int betweenCycleDelay;

  public Connection connection;
  public Device device;

  /**
   * Given a config file, initialize the importer for use.
   * 
   * @param importerClass class name for importer
   * @param configFile path to config file
   * @param verbose true for info, false for severe
   */
  public void initialize(String importerClass, String configFile, boolean verbose) {

    // process the config file
    processConfigFile(configFile);
  }

  /**
   * Disconnects from the database.
   */
  public void deinitialize() {
    sqlDataSource.disconnect();
  }

  /**
   * Parse configuration file. This sets class variables used in the importing process
   * 
   * @param configFile name of the config file
   */
  public void processConfigFile(String configFile) {

    LOGGER.info("Reading config file {}", configFile);

    // initialize the config file and verify that it was read
    params = new ConfigFile(configFile);
    if (!params.wasSuccessfullyRead()) {
      LOGGER.error("{} was not successfully read", configFile);
      System.exit(-1);
    }

    // get the vdx parameter, and exit if it's missing
    vdxConfig = StringUtils.stringToString(params.getString("vdx.config"), "VDX.config");
    if (vdxConfig == null) {
      LOGGER.error("vdx.config parameter missing from config file");
      System.exit(-1);
    }

    // get the vdx config as it's own config file object
    vdxParams = new ConfigFile(vdxConfig);
    driver = vdxParams.getString("vdx.driver");
    url = vdxParams.getString("vdx.url");
    prefix = vdxParams.getString("vdx.prefix");

    // define a format for log message dates
    dateOut = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));

    // get connection settings related to this instance
    postConnectDelay = StringUtils.stringToInt(params.getString("postConnectDelay"), 1000);
    betweenPollDelay = StringUtils.stringToInt(params.getString("betweenPollDelay"), 1000);
    betweenCycleDelay = StringUtils.stringToInt(params.getString("betweenCycleDelay"), 1000);

    // get the rank configuration for this import. there can only be a single rank per import
    rankParams = params.getSubConfig("rank");
    rankName = StringUtils.stringToString(rankParams.getString("name"), "Raw Data");
    rankValue = StringUtils.stringToInt(rankParams.getString("value"), 1);
    rankDefault = StringUtils.stringToInt(rankParams.getString("default"), 0);
    rank = new Rank(0, rankName, rankValue, rankDefault);
    LOGGER.info("[Rank] " + rankName);
    LOGGER.info("");

    // get the channel configurations for this import. there can be multiple channels per import
    channelMap = new HashMap<String, Channel>();
    stringList = params.getList("channel");
    for (int i = 0; i < stringList.size(); i++) {
      channelCode = stringList.get(i);
      channelParams = params.getSubConfig(channelCode);
      channelName = StringUtils.stringToString(channelParams.getString("name"), channelCode);
      channelLon = StringUtils.stringToDouble(channelParams.getString("longitude"), Double.NaN);
      channelLat = StringUtils.stringToDouble(channelParams.getString("latitude"), Double.NaN);
      channelHeight = StringUtils.stringToDouble(channelParams.getString("height"), Double.NaN);
      channelActive = StringUtils.stringToInt(channelParams.getString("active"), 1);
      channel = new Channel(0, channelCode, channelName, channelLon, channelLat, channelHeight,
          channelActive);
      channelMap.put(channelCode, channel);
    }

    // define the station objects to store station configurations
    stationList = new ArrayList<String>();
    channelList = new ArrayList<String>();
    stationChannelMap = new HashMap<String, String>();
    stationDeviceMap = new HashMap<String, Device>();
    stationConnectionParamsMap = new HashMap<String, ConfigFile>();
    stationTimesourceMap = new HashMap<String, String>();
    stationLastDataTimeMap = new HashMap<String, Date>();

    // validate that station are defined in the config file
    stringList = params.getList("station");
    if (stringList == null) {
      LOGGER.error("station parameter(s) missing from config file");
      System.exit(-1);
    }

    // get the list of station that are being used in this import
    for (int i = 0; i < stringList.size(); i++) {

      // station configuration
      stationCode = stringList.get(i);
      stationParams = params.getSubConfig(stationCode);
      deviceParams = stationParams.getSubConfig("device");
      connectionParams = stationParams.getSubConfig("connection");
      channelCode = StringUtils.stringToString(stationParams.getString("channel"), stationCode);
      timesource = stationParams.getString("timesource");

      // verify that the time source is configured for this station
      if (timesource == null) {
        LOGGER.error("timesource parameter for {} missing from config file", stationCode);
        System.exit(-1);
      }

      // try to create a connection object
      try {
        Class<?> connClass = Class.forName(connectionParams.getString("driver"));
        Constructor<?> cnst = connClass.getConstructor(new Class[] {String.class});
        connection = (Connection) cnst.newInstance(new Object[] {stationCode});
        connection.initialize(connectionParams);
      } catch (Exception e) {
        LOGGER.error("Connection initialization failed");
        continue;
      }

      // try to create a device object
      try {
        device = (Device) Class.forName(deviceParams.getString("driver")).newInstance();
        device.initialize(deviceParams);
      } catch (Exception e) {
        LOGGER.error("Device driver initialization failed");
        System.exit(-1);
      }

      stationList.add(stationCode);
      channelList.add(channelCode);
      stationChannelMap.put(stationCode, channelCode);
      stationDeviceMap.put(stationCode, device);
      stationConnectionParamsMap.put(stationCode, connectionParams);
      stationTimesourceMap.put(stationCode, timesource);
      stationLastDataTimeMap.put(stationCode, null);

      // display configuration information related to this station
      LOGGER.info("[Station] {}", stationCode);
      LOGGER.info("[Connection] {}", connection.toString());
      LOGGER.info("[ConnDriver] {}", connectionParams.getString("driver"));
      LOGGER.info("[Device] {}", device.toString());
      LOGGER.info("[DevDriver] {}", deviceParams.getString("driver"));
      LOGGER.info("[Fields] {}", device.getFields());
      LOGGER.info("[Channel] {}", channelCode);
      LOGGER.info("[Timesource] {}", timesource);
      LOGGER.info("");

      // destroy this temporary connection
      connection = null;
    }

    // define a comma separated list of channels affected in this import
    channelList = new ArrayList<String>(new HashSet<String>(channelList));
    defaultChannels = "";
    for (int i = 0; i < channelList.size(); i++) {
      defaultChannels += channelList.get(i) + ",";
    }
    defaultChannels = defaultChannels.substring(0, defaultChannels.length() - 1);
    LOGGER.info("[defaultChannels] {}", defaultChannels);

    // validate that data sources are defined in the config file
    dataSourceList = params.getList("dataSource");
    if (dataSourceList == null) {
      LOGGER.error("dataSource parameter(s) missing from config file");
      System.exit(-1);
    }

    // define the data source handler that acts as a wrapper for data sources
    sqlDataSourceHandler = new SQLDataSourceHandler(driver, url, prefix);
    sqlDataSourceMap = new HashMap<String, SQLDataSource>();
    dataSourceChannelMap = new HashMap<String, String>();
    dataSourceColumnMap = new HashMap<String, String>();
    dataSourceRIDMap = new HashMap<String, Integer>();

    // iterate through each of the data sources and setup the db for it
    for (int i = 0; i < dataSourceList.size(); i++) {

      // get the data source name
      dataSource = dataSourceList.get(i);
      LOGGER.info("[DataSource] {}", dataSource);

      // lookup the data source from the list that is in vdxSources.config
      sqlDataSourceDescriptor = sqlDataSourceHandler.getDataSourceDescriptor(dataSource);
      if (sqlDataSourceDescriptor == null) {
        LOGGER.error("{} not in vdxSources.config - Skipping", dataSource);
        continue;
      }

      // formally get the data source from the list of descriptors. this will initialize the data
      // source which includes db creation
      sqlDataSource = sqlDataSourceDescriptor.getSQLDataSource();

      // store the reference to the initialized data source in the map of initialized data sources
      sqlDataSourceMap.put(dataSource, sqlDataSource);

      // get the config for this data source
      dataSourceParams = params.getSubConfig(dataSource);

      // if this is a ranked data source, then create the rank in the database
      if (sqlDataSource.getRanksFlag()) {
        Rank tempRank = sqlDataSource.defaultGetRank(rank);
        if (tempRank == null) {
          tempRank = sqlDataSource.defaultInsertRank(rank);
        }
        if (tempRank == null) {
          LOGGER.error("{} {} rank creation failed", dataSource, rank.getName());
          System.exit(-1);
        }
        dataSourceRIDMap.put(dataSource, tempRank.getId());
      }

      // columns based configuration
      if (sqlDataSource.getColumnsFlag()) {

        // look up columns from the config file and try to insert them into the database
        stringList = dataSourceParams.getList("column");
        if (stringList != null) {
          for (int j = 0; j < stringList.size(); j++) {
            columnName = stringList.get(j);
            columnParams = dataSourceParams.getSubConfig(columnName);
            columnIdx = StringUtils.stringToInt(columnParams.getString("idx"), i);
            columnDescription =
                StringUtils.stringToString(columnParams.getString("description"), columnName);
            columnUnit = StringUtils.stringToString(columnParams.getString("unit"), columnName);
            columnChecked = StringUtils.stringToBoolean(columnParams.getString("checked"), false);
            columnActive = StringUtils.stringToBoolean(columnParams.getString("active"), true);
            columnBypass = StringUtils.stringToBoolean(columnParams.getString("bypass"), false);
            columnAccumulate = StringUtils.stringToBoolean(columnParams.getString("accumulate"), false);
            column = new Column(columnIdx, columnName, columnDescription, columnUnit, columnChecked,
                columnActive, columnBypass, columnAccumulate);
            if (sqlDataSource.defaultGetColumn(columnName) == null) {
              sqlDataSource.defaultInsertColumn(column);
            }
          }
        }

        // generate a list of all the columns in the database for this data source
        columns = "";
        columnList = sqlDataSource.defaultGetColumns(true, false);
        for (int j = 0; j < columnList.size(); j++) {
          columns += columnList.get(j).name + ",";
        }
        columns = columns.substring(0, columns.length() - 1);
        columns = StringUtils.stringToString(dataSourceParams.getString("columns"), columns);
        dataSourceColumnMap.put(dataSource, columns);
        LOGGER.info("[Columns] {}", columns);
      }

      // create translations table which is based on column entries
      if (sqlDataSource.getTranslationsFlag()) {
        sqlDataSource.defaultCreateTranslation();
      }

      // get the channels for this data source
      channels = StringUtils.stringToString(dataSourceParams.getString("channels"), defaultChannels);
      dataSourceChannelMap.put(dataSource, channels);
      LOGGER.info("[Channels] {}", channels);

      // create channels tables for this data source
      if (sqlDataSource.getChannelsFlag()) {
        channelArray = channels.split(",");
        for (int j = 0; j < channelArray.length; j++) {
          channelCode = channelArray[j];
          channel = channelMap.get(channelCode);
          channelParams = params.getSubConfig(channelCode);

          if (channel == null) {
            continue;
          }

          // if the channel doesn't exist then create it with the default tid of 1
          if (sqlDataSource.defaultGetChannel(channel.getCode(),
              sqlDataSource.getChannelTypesFlag()) == null) {
            if (sqlDataSource.getType().equals("tilt")) {
              azimuthNom = StringUtils.stringToDouble(channelParams.getString("azimuth"), 0);
              sqlDataSource.defaultCreateTiltChannel(channel, 1, azimuthNom,
                  sqlDataSource.getChannelsFlag(), sqlDataSource.getTranslationsFlag(),
                  sqlDataSource.getRanksFlag(), sqlDataSource.getColumnsFlag());
            } else {
              sqlDataSource.defaultCreateChannel(channel, 1, sqlDataSource.getChannelsFlag(),
                  sqlDataSource.getTranslationsFlag(), sqlDataSource.getRanksFlag(),
                  sqlDataSource.getColumnsFlag());
            }

            // retrieve the new channel and store it off
            channel = sqlDataSource.defaultGetChannel(channel.getCode(),
                sqlDataSource.getChannelTypesFlag());
            channelMap.put(channelCode, channel);
          }

          // create a new translation if any non-default values were specified, and use the new tid
          // for the create channel statement
          if (sqlDataSource.getTranslationsFlag()) {
            int extraColumn = 0;
            double multiplier;
            double offset;

            // get the translations sub config for this channel
            translationParams = channelParams.getSubConfig("translation");

            // apply an offset if this is a tilt data source to include the installation azimuth
            if (sqlDataSource.getType().equals("tilt")) {
              extraColumn = 1;
            }

            // create a matrix to store the translation data
            DoubleMatrix2D dm = DoubleFactory2D.dense.make(1, columnList.size() * 2 + extraColumn);
            String[] columnNames = new String[columnList.size() * 2 + extraColumn];

            // save the installation azimuth if this is a tilt data source
            if (sqlDataSource.getType().equals("tilt")) {
              azimuthInst = StringUtils.stringToDouble(translationParams.getString("azimuth"), 0);
              dm.setQuick(0, columnList.size() * 2, azimuthInst);
              columnNames[columnList.size() * 2] = "azimuth";
            }

            // iterate through the column list to get the translation values
            for (int k = 0; k < columnList.size(); k++) {
              column = columnList.get(k);
              columnName = column.name;
              multiplier = StringUtils.stringToDouble(translationParams.getString("c" + columnName), 1);
              offset = StringUtils.stringToDouble(translationParams.getString("d" + columnName), 0);
              dm.setQuick(0, k * 2, multiplier);
              dm.setQuick(0, k * 2 + 1, offset);
              columnNames[k * 2] = "c" + columnName;
              columnNames[k * 2 + 1] = "d" + columnName;
            }

            GenericDataMatrix gdm = new GenericDataMatrix(dm);
            gdm.setColumnNames(columnNames);

            int tid = 1;
            tid = sqlDataSource.defaultGetTranslation(channel.getCode(), gdm);
            if (tid == 1) {
              tid = sqlDataSource.defaultInsertTranslation(channel.getCode(), gdm);
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
   * Parse file from url (resource locator or file name).
   * 
   * @param filename file to be parsed
   */
  public void process(String filename) {

    while (true) {

      // output initial polling message
      LOGGER.info("");
      LOGGER.info("BEGIN POLLING CYCLE");

      // initialize variables
      int tries;
      int lineNumber;
      boolean done;
      String line;

      // iterate through each of the channels
      for (int c = 0; c < stationList.size(); c++) {

        // get the station name, and it's associated configuration
        stationCode = stationList.get(c);
        channelCode = stationChannelMap.get(stationCode);
        device = stationDeviceMap.get(stationCode);
        connectionParams = stationConnectionParamsMap.get(stationCode);
        timesource = stationTimesourceMap.get(stationCode);

        // get the import line definition for this channel
        fieldArray = device.getFields().split(",");
        fieldMap = new HashMap<Integer, String>();
        for (int i = 0; i < fieldArray.length; i++) {
          fieldMap.put(i, fieldArray[i].trim());
        }

        // get the latest data time from data source that keeps track of time
        if (stationLastDataTimeMap.get(stationCode) == null) {
          sqlDataSource = sqlDataSourceMap.get(timesource);
          lastDataTime = sqlDataSource.defaultGetLastDataTime(channelCode, device.getNullfield(),
              device.getPollhist());
          stationLastDataTimeMap.put(stationCode, lastDataTime);
        }
        lastDataTime = stationLastDataTimeMap.get(stationCode);

        // initialize data objects related to this device
        dateIn = new SimpleDateFormat(device.getTimestamp());
        dateIn.setTimeZone(TimeZone.getTimeZone(device.getTimezone()));

        // default some variables used in the loop
        tries = 0;
        lineNumber = 0;
        line = "";
        done = false;

        // iterate through the maximum number of retries as specified in the config file
        while (tries < device.getMaxtries() && !done) {

          // increment the tries variable
          tries++;

          // display logging information
          LOGGER.info("");
          LOGGER.info("Polling {} [Try {}/{}] [lastDataTime: {}]", stationCode, tries,
              device.getMaxtries(), dateOut.format(lastDataTime));

          // create a connection to the station
          try {
            Class<?> connClass = Class.forName(connectionParams.getString("driver"));
            Constructor<?> cnst = connClass.getConstructor(new Class[] {String.class});
            connection = (Connection) cnst.newInstance(new Object[] {stationCode});
            connection.initialize(connectionParams);
          } catch (Exception e) {
            LOGGER.error("Connection initialization failed", e);
            continue;
          }

          // connect to the device
          try {
            connection.connect();
            Thread.sleep(postConnectDelay);
          } catch (Exception e) {
            LOGGER.error("Station Connection failed", e);
            if (connection.isOpen()) {
              connection.disconnect();
            }
            continue;
          }

          // try to build the data request string
          String dataRequest = "";
          try {
            dataRequest = device.requestData(lastDataTime);
          } catch (Exception e) {
            LOGGER.error("Device build request failed", e);
            connection.disconnect();
            continue;
          }

          // send the request to the device
          if (dataRequest.length() > 0) {
            try {
              connection.writeString(dataRequest);
              LOGGER.info("dataRequest: {}", dataRequest);
            } catch (Exception e) {
              LOGGER.error("Connection send data request failed", e);
              connection.disconnect();
              continue;
            }
          }

          // try wait (eh) for the response from the device (clear out the message queue first)
          String dataResponse = "";
          try {
            connection.emptyMsgQueue();
            dataResponse = connection.readString(device);
            // LOGGER.log(Level.INFO, "dataResponse:" + dataResponse);
          } catch (Exception e) {
            LOGGER.error("Device receive data response failed", e);
            connection.disconnect();
            continue;
          }

          // try to validate the response from the device
          try {
            device.validateMessage(dataResponse, true);
          } catch (Exception e) {
            LOGGER.error("Message validation failed", e);
            connection.disconnect();
            continue;
          }

          // we can now disconnect from the device
          connection.disconnect();

          // format the response based on the type of device
          String dataMessage = device.formatMessage(dataResponse);
          // LOGGER.log(Level.INFO, "dataMessage:\n" + dataMessage);

          // parse the response by lines
          StringTokenizer st = new StringTokenizer(dataMessage, "\n");

          // reset the counter variables
          lineNumber = 0;

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
              LOGGER.info("invalid: {}", line);
              continue;
            }

            // format this data line
            line = device.formatLine(line);

            // output this line to the log file
            LOGGER.info("{}", line);

            // split the data row into an ordered list. be sure to use the two argument split, as
            // some lines may have many trailing delimiters
            Pattern p = Pattern.compile(device.getDelimiter());
            String[] valueArray = p.split(line, -1);
            HashMap<Integer, String> valueMap = new HashMap<Integer, String>();
            for (int i = 0; i < valueArray.length; i++) {
              valueMap.put(i, valueArray[i].trim());
            }

            // make sure the data row matches the defined data columns
            if (fieldMap.size() > valueMap.size()) {
              LOGGER.error("line {} has too few values: {}", lineNumber, line);
              continue;
            }

            // map the columns to the values. look for the TIMESTAMP and CHANNEL flags
            HashMap<Integer, ColumnValue> columnValueMap = new HashMap<Integer, ColumnValue>();
            ColumnValue columnValue;
            String name;
            double value;
            int count = 0;
            String tsValue = "";

            // try to parse the values from this data line
            try {
              for (int i = 0; i < fieldMap.size(); i++) {
                name = fieldMap.get(i);

                // skip IGNORE columns
                if (name.equals("IGNORE")) {
                  continue;

                  // parse out the CHANNEL
                } else if (name.equals("CHANNEL")) {
                  channelCode = valueMap.get(i);
                  continue;

                  // parse out the TIMESTAMP
                } else if (name.equals("TIMESTAMP")) {
                  tsValue += valueMap.get(i) + " ";
                  continue;

                  // elements that are neither IGNORE nor CHANNELS nor TIMESTAMPS are DATA
                } else {
                  if (valueMap.get(i).length() == 0) {
                    value = Double.NaN;
                  } else {
                    value = Double.parseDouble(valueMap.get(i));
                  }
                  columnValue = new ColumnValue(name, value);
                  columnValueMap.put(count, columnValue);
                  count++;
                }
              }

              // any problems with parsing the values for this line should be caught here
            } catch (Exception e) {
              LOGGER.error("line {} parse error", lineNumber, e);
              continue;
            }

            // make sure that the timestamp has something in it
            if (tsValue.length() == 0) {
              LOGGER.error("line {} timestamp not found", lineNumber);
              continue;
            }

            // convert the time zone of the input date and convert to j2ksec
            try {
              String timestamp = tsValue.trim();
              date = dateIn.parse(timestamp);
              j2ksec = J2kSec.fromDate(date);
            } catch (ParseException e) {
              LOGGER.error("line {} timestamp parse error", lineNumber);
              continue;
            }

            ColumnValue tsColumn = new ColumnValue("j2ksec", j2ksec);

            // define the last data time for this download
            if (J2kSec.asDate(j2ksec).after(stationLastDataTimeMap.get(stationCode))) {
              stationLastDataTimeMap.put(stationCode, J2kSec.asDate(j2ksec));
            }

            // iterate through each data source that was defined and assign data from this line to
            // it
            for (int i = 0; i < dataSourceList.size(); i++) {

              // get the data source name and it's associated sql data source
              dataSource = dataSourceList.get(i);
              channels = dataSourceChannelMap.get(dataSource);
              dsChannelArray = channels.split(",");

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
              sqlDataSource = sqlDataSourceMap.get(dataSource);
              if (sqlDataSource == null) {
                LOGGER.error("line {} data source {} not initialized", lineNumber, dataSource);
                continue;
              }

              // channel for this data source. create it if it doesn't exist
              if (sqlDataSource.getChannelsFlag()) {
                if (sqlDataSource.defaultGetChannel(channelCode,
                    sqlDataSource.getChannelTypesFlag()) == null) {
                  sqlDataSource.defaultCreateChannel(
                      new Channel(0, channelCode, channelCode, Double.NaN, Double.NaN, Double.NaN,
                          1),
                      1, sqlDataSource.getChannelsFlag(), sqlDataSource.getTranslationsFlag(),
                      sqlDataSource.getRanksFlag(), sqlDataSource.getColumnsFlag());
                }
              }

              // columns for this data source
              columns = dataSourceColumnMap.get(dataSource);
              columnArray = columns.split(",");
              HashMap<Integer, String> dsColumnMap = new HashMap<Integer, String>();
              for (int j = 0; j < columnArray.length; j++) {
                dsColumnMap.put(j, columnArray[j]);
              }

              // rank for this data source. this should already exist in the database
              if (sqlDataSource.getRanksFlag()) {
                rid = dataSourceRIDMap.get(dataSource);
              } else {
                rid = 1;
              }

              // create a data entry map for this data source, with the columns that it wants
              HashMap<Integer, ColumnValue> dataSourceEntryMap =
                  new HashMap<Integer, ColumnValue>();
              count = 0;
              dataSourceEntryMap.put(count, tsColumn);
              count++;

              // store the remaining data columns
              for (int j = 0; j < columnValueMap.size(); j++) {
                columnValue = columnValueMap.get(j);
                name = columnValue.columnName;
                value = columnValue.columnValue;
                for (int k = 0; k < dsColumnMap.size(); k++) {
                  if (name.equals(dsColumnMap.get(k))) {
                    dataSourceEntryMap.put(count, columnValue);
                    count++;
                  }
                }
              }

              // put the list of entries a double matrix and create a column names array
              DoubleMatrix2D dm = DoubleFactory2D.dense.make(1, dataSourceEntryMap.size());
              String[] columnNames = new String[dataSourceEntryMap.size()];
              for (int j = 0; j < dataSourceEntryMap.size(); j++) {
                columnValue = dataSourceEntryMap.get(j);
                name = columnValue.columnName;
                value = columnValue.columnValue;
                columnNames[j] = name;
                dm.setQuick(0, j, value);
              }

              if (columnNames.length == 1 && columnNames[0].equalsIgnoreCase("j2ksec")) {
                continue;
              }

              // assign the double matrix and column names to a generic data matrix
              GenericDataMatrix gdm = new GenericDataMatrix(dm);
              gdm.setColumnNames(columnNames);

              // insert the data to the database
              sqlDataSource.defaultInsertData(channelCode, gdm, sqlDataSource.getTranslationsFlag(),
                  sqlDataSource.getRanksFlag(), rid);
            }

            // if we made it here then no exceptions were thrown, then we got the data
            done = true;
          }
        }

        // output a status message based on how everything went above
        if (done) {
          LOGGER.info("Polling {} Success", stationCode);
        } else {
          LOGGER.info("Polling {} Failure", stationCode);
        }

        // try to sleep before accessing the next station
        try {
          Thread.sleep(betweenPollDelay);
        } catch (Exception e) {
          LOGGER.error("Thread.sleep(betweenPollDelay) failed.", e);
        }
      }
      LOGGER.info("");
      LOGGER.info("END POLLING CYCLE");

      // try to sleep before going to the next polling cycle
      try {
        Thread.sleep(betweenCycleDelay);
      } catch (Exception e) {
        LOGGER.error("Thread.sleep(betweenCycleDelay) failed.", e);
      }
    }
  }

  /**
   * Print instructions to the screen.
   */
  public void outputInstructions(String importerClass, String message) {
    if (message != null) {
      System.err.println(message);
    }
    System.err.println(importerClass + " -c configfile");
  }

  /**
   * Main method. Command line syntax: -h, --help print help message -c config file name -v verbose
   * mode files ...
   */
  public static void main(String[] as) {

    ImportPoll importer = new ImportPoll();

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
