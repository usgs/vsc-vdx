package gov.usgs.vdx.in;

import java.io.*;
import java.util.*;
import java.text.*;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

// import gov.usgs.pipe.*;
import gov.usgs.util.*;
import gov.usgs.vdx.data.GenericDataMatrix;
import gov.usgs.vdx.data.generic.fixed.SQLGenericFixedDataSource;
import gov.usgs.vdx.data.tilt.SQLTiltStationDataSource;


/**
 * A program to stream data from an ip device and put it in the database
 *
 * @author Loren Antolik
 */
public class DataStreamer {
	
	// configFile: main data poller config file
	private ConfigFile configFile;
	// configFileName: full path and name of VDX config file
	private String vdxConfig;
	// vdxName: database prefix
	private String vdxName;
	// vdxParams: values from VDX.config
	private ConfigFile vdxParams;
	// stationParams: specific to each station
	private ConfigFile stationParams;
	// dataTypeParams: specific to each data set of a station
	private ConfigFile dataTypeParams;
	
	// currentTime: object for accessing the current time
	private CurrentTime currentTime = CurrentTime.getInstance();
	// dateIn, dateOut: format of the date from the device
	private SimpleDateFormat dateIn		= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat dateOut	= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	// outputFile: name of file to log to, will default to config file basename
	private String outputFile;
	// outputLevel: amount of logging to output (NONE, LOW, FULL)
	private int outputLevel = FULL;
	// postConnectDelay: time in milliseconds to wait before sending first message
	private int postConnectDelay;
	// betweenPollDelay: time in milliseconds to wait before sending next message
	// private int betweenPollDelay;
	// deviceIP: the IP address of the repeater radio
	private String deviceIP;
	// devicePort: the port of the repeater radio used
	private int devicePort;
	
	// stationObject: an object which represents a station - parameters and data types
	private Station stationObject;
	
	// station: the station code, usually a three letter code
	private String station;
	// stationType: type of sensors at the stations (tilt, strain, gas) (for now)
	private String stationType;
	// name: the station name, usually a descriptive name for the station
	private String name;
	// tiltid: the id of the station in the database
	private int tiltid;
	// latitude, longitude: the station location, in decimal degrees
	private double latitude, longitude;
	// nominal azimuth
	private double azimuthNom;
	// tilt translation variables: used to create an entry in the translations table
	private double cx, dx, cy, dy, ch, dh, cb, db, ci, di, cg, dg, cr, dr, azimuthInst;
	// connectionTimeout: time period in milliseconds to wait for an initial response
	private int connTimeout;
	// dataTimeout: time period in milliseconds to wait for a data request response
	private int dataTimeout;
	// maxRetries: number of times to retry a connection
	private int maxRetries;
	// lastDataTimeSource: the most recent time of a data point in the database
	private String timeSource;
	// syncInterval: the amount of time between syncing the clock at the station
	private int syncInterval;
	// instrument: the type of instrument at the station
	private String instrument;
	// delimiter: the character that delimits fields in a message
	private String delimiter;
	
	// dataTypes: a list of the data types for a station
	private List<String> dataTypes;
	// dataTypeIterator: a data structure to parse through the data types for a station
	private Iterator<String> dataTypeIterator;
	// dataTypeObject: an object which represents a data type
	private DataType dataTypeObject;
	// dataTypeList: a list holding all data type objects for a station
	private List<DataType> dataTypeList;
	
	// dataType: label for this collection of data within the data line
	private String dataType;
	// order: order for this collection of data within the data line
	private int order;
	// type: type of data in this collection
	private String type;
	// format: format of data in this collection
	private String format;
	// rank: database rank of data in this collection
	private int rank;
	
	// data sources
	private SQLTiltStationDataSource dataSource;
	
	// NONE: no logging will take place
	private static final int NONE = 0;
	// LOW: normal program operation logging
	private static final int LOW = 1;
	// FULL: debugging logging
	private static final int FULL = 2;
	
	/**
	 * default constructor
	 */
	public DataStreamer () {
		dateIn.setTimeZone(TimeZone.getTimeZone("GMT"));
		dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	/**
	 * constructor that is used by main
	 * 
	 * @param cf		config file name
	 * @param doStream	flag to stream the stations
	 */
	public DataStreamer (String cf, boolean doStream) {
		
		this();
		processConfigFile(cf);
		prepareStreaming();
		
		if (doStream) {
			// setName("DataStreamer [" + configFile.getName() + "]");
			startStreaming();
		}
		
		System.exit(0);
	}
	
	/**
	 * reads the config file and parses values variables
	 * 
	 * @param cf
	 */
	public void processConfigFile(String cf) {
		
		// parse the config file
		configFile = new ConfigFile(cf);
		
		// get the database connection parameters
		vdxConfig = Util.stringToString(configFile.getString("vdxConfig"), "VDX.config");
		vdxParams = new ConfigFile(vdxConfig);
		
		// get the output file information for logging
		outputFile	= Util.stringToString(configFile.getString("outputFile"), getLogFile(configFile.getName()));		
		outputLevel	= Util.stringToInt(configFile.getString("outputLevel"), LOW);
		
		// output a header string
		output("###################################################################################", FULL, true);
		output("#### BEGIN " + outputFile + " (" + outputLevel + ") " + currentTime.nowString(), FULL, true);
		output("###################################################################################\n", FULL, true);
		
		// output more database settings
		output("#### Database Settings ####", FULL, true);
		vdxName	= Util.stringToString(configFile.getString("vdxName"), "");
		output("vdxName:" + vdxName, FULL, true);
		output("", FULL, true);

		// output the connection settings
		output("#### Connection Settings ####", FULL, true);
		deviceIP			= Util.stringToString(configFile.getString("deviceIP"), "0.0.0.0");
		output("deviceIP:" + deviceIP, FULL, false);		
		devicePort			= Util.stringToInt(configFile.getString("devicePort"), 0);
		output(" devicePort:" + String.valueOf(devicePort), FULL, false);
		postConnectDelay	= Util.stringToInt(configFile.getString("postConnectDelay"), 500);
		output(" postConnectDelay:" + String.valueOf(postConnectDelay) + "ms", FULL, true);		
		// betweenPollDelay	= Util.stringToInt(configFile.getString("betweenPollDelay"), 500);
		// output(" betweenPollDelay:" + String.valueOf(betweenPollDelay) + "ms", FULL, true);
		output("", FULL, true);
		
		// get the list of all the stations to poll, create an iterator to parse through the sub configs and save them in a list
		station				= configFile.getString("station");
		output("#### Station Settings", FULL, true);

		output("station:" + station, FULL, false);
			
		// parse the config file for this station
		stationParams	= configFile.getSubConfig(station);
			
		// station settings for the channels table
		stationType	= Util.stringToString(stationParams.getString("stationType"), "tilt");
		output(" stationType:" + String.valueOf(stationType), FULL, false);
		name		= Util.stringToString(stationParams.getString("name"), station);
		output(" name:" + String.valueOf(name), FULL, false);
		tiltid		= Util.stringToInt(stationParams.getString("tiltid"), 0);
		output(" tiltid:" + String.valueOf(tiltid), FULL, false);
		latitude	= Util.stringToDouble(stationParams.getString("latitude"), 19.0);
		output(" latitude:" + String.valueOf(latitude), FULL, false);
		longitude	= Util.stringToDouble(stationParams.getString("longitude"), -155.0);
		output(" longitude:" + String.valueOf(longitude), FULL, false);
		azimuthNom	= Util.stringToDouble(stationParams.getString("azimuthNom"), 0.0);
		output(" azimuthNom:" + String.valueOf(azimuthNom), FULL, true);
			
		// tilt translations, offsets and defaults
		cx		= Util.stringToDouble(stationParams.getString("cx"), 1.0);
		output("cx:" + String.valueOf(cx), FULL, false);
		dx		= Util.stringToDouble(stationParams.getString("dx"), 0.0);
		output(" dx:" + String.valueOf(dx), FULL, false);
		cy		= Util.stringToDouble(stationParams.getString("cy"), 1.0);
		output(" cy:" + String.valueOf(cy), FULL, false);
		dy		= Util.stringToDouble(stationParams.getString("dy"), 0.0);
		output(" dy:" + String.valueOf(dy), FULL, false);
		ch		= Util.stringToDouble(stationParams.getString("ch"), 1.0);
		output(" ch:" + String.valueOf(ch), FULL, false);
		dh		= Util.stringToDouble(stationParams.getString("dh"), 0.0);
		output(" dh:" + String.valueOf(dh), FULL, false);
		cb		= Util.stringToDouble(stationParams.getString("cb"), 1.0);
		output(" cb:" + String.valueOf(cb), FULL, false);
		db		= Util.stringToDouble(stationParams.getString("db"), 0.0);
		output(" db:" + String.valueOf(db), FULL, false);
		ci		= Util.stringToDouble(stationParams.getString("ci"), 1.0);
		output(" ci:" + String.valueOf(ci), FULL, false);
		di		= Util.stringToDouble(stationParams.getString("di"), 0.0);
		output(" di:" + String.valueOf(di), FULL, false);
		cg		= Util.stringToDouble(stationParams.getString("cg"), 1.0);
		output(" cg:" + String.valueOf(cg), FULL, false);
		dg		= Util.stringToDouble(stationParams.getString("dg"), 0.0);
		output(" dg:" + String.valueOf(dg), FULL, false);
		cr		= Util.stringToDouble(stationParams.getString("cr"), 1.0);
		output(" cr:" + String.valueOf(cr), FULL, false);
		dr		= Util.stringToDouble(stationParams.getString("dr"), 0.0);
		output(" dr:" + String.valueOf(dr), FULL, false);
		azimuthInst	= Util.stringToDouble(stationParams.getString("azimuthInst"), 0.0);
		output(" azimuthInst:" + String.valueOf(azimuthInst), FULL, true);
			
		// connection settings	
		connTimeout		= Util.stringToInt(stationParams.getString("connTimeout"), 2000);
		output(" connTimeout:" + String.valueOf(connTimeout) + "ms", FULL, false);			
		dataTimeout		= Util.stringToInt(stationParams.getString("dataTimeout"), 2000);
		output(" dataTimeout:" + String.valueOf(dataTimeout) + "ms", FULL, false);			
		maxRetries		= Util.stringToInt(stationParams.getString("maxRetries"), 2);
		output(" maxRetries:" + String.valueOf(maxRetries), FULL, false);			
		timeSource		= Util.stringToString(stationParams.getString("timeSource"), "tilt");
		output(" timeSource:" + timeSource, FULL, false);			
		syncInterval	= Util.stringToInt(stationParams.getString("syncInterval"), 0);
		output(" syncInterval:" + syncInterval, FULL, true);		
		instrument		= Util.stringToString(stationParams.getString("instrument"), "zeno");
		output(" instrument:" + instrument, FULL, false);			
		delimiter		= Util.stringToString(stationParams.getString("delimiter"), ",");
		output(" delimiter:" + delimiter, FULL, true);
			
		// get the list of all the data types for this station, create an iterator 
		// to parse through the sub configs and save them in a list
		dataTypes			= stationParams.getList("dataType");
		dataTypeList		= new ArrayList<DataType>();
			
		// if data types exist then parse them into a dataTypeObject
		if (dataTypes != null) {
			dataTypeIterator	= dataTypes.iterator();
				
			// iterate through each of the data types for this station
			while (dataTypeIterator.hasNext()) {
					
				// iterate to the next data type
				dataType	= dataTypeIterator.next();
				output("dataType:" + dataType, FULL, false);
					
				// parse the config file for this data type
				dataTypeParams	= stationParams.getSubConfig(dataType);
					
				// get the data type specific settings
				order	= Util.stringToInt(dataTypeParams.getString("order"), 0);
				output(" order:" + String.valueOf(order), FULL, false);					
				rank	= Util.stringToInt(dataTypeParams.getString("rank"), 0);
				output(" rank:" + String.valueOf(rank), FULL, false);
				type	= Util.stringToString(dataTypeParams.getString("type"), "data");
				output(" type:" + type, FULL, false);				
				format	= Util.stringToString(dataTypeParams.getString("format"), "");
				output(" format:" + format, FULL, true);
					
				// parse the values into a data type object
				dataTypeObject = new DataType(dataType, order, type, format, rank);
					
				// place the object into the data type array
				dataTypeList.add(dataTypeObject);
			}
		}
			
		// parse the values into a station object
		stationObject		= new Station(station, stationType, name, tiltid, latitude, longitude, azimuthNom,
			                         cx, dx, cy, dy, ch, dh, cb, db, ci, di, cg, dg, cr, dr, azimuthInst, 
			                         1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 
			                         0, 0, connTimeout, dataTimeout, maxRetries, timeSource, syncInterval,
			                         0, instrument, delimiter, dataTypeList);
			
		// output a blank line for the log file
		output("", FULL, true);
		
	}
	
	/**
	 * sets up the channels and translations if they do not exist and defaults the channels table to these translations
	 */
	public void prepareStreaming() {
		
		vdxParams.put("vdx.name", vdxName, false);
		dataSource = new SQLTiltStationDataSource();
		// TODO: work out new initialization
		// dataSource.initialize(vdxParams);
		DoubleMatrix2D dm	= DoubleFactory2D.dense.make(1, 15);
		dm.setQuick(0, 0, stationObject.cx);
		dm.setQuick(0, 0, stationObject.dx);
		dm.setQuick(0, 0, stationObject.cy);
		dm.setQuick(0, 0, stationObject.dy);
		dm.setQuick(0, 0, stationObject.ch);
		dm.setQuick(0, 0, stationObject.dh);
		dm.setQuick(0, 0, stationObject.cb);
		dm.setQuick(0, 0, stationObject.db);
		dm.setQuick(0, 0, stationObject.ci);
		dm.setQuick(0, 0, stationObject.di);
		dm.setQuick(0, 0, stationObject.cg);
		dm.setQuick(0, 0, stationObject.dg);
		dm.setQuick(0, 0, stationObject.cr);
		dm.setQuick(0, 0, stationObject.dr);
		dm.setQuick(0, 0, stationObject.azimuthInst);
		GenericDataMatrix gdm = new GenericDataMatrix(dm);
		gdm.setColumnNames(new String[] {"cxTilt", "dxTilt", "cyTilt", "dyTilt", "choleTemp", "dholeTemp", "cboxTemp", "dboxTemp",
				"cinstVolt", "dinstVolt", "cgndVolt", "dgndVolt", "crain", "drain", "azimuth"});
		dataSource.createChannel(stationObject.station, stationObject.name, 
				stationObject.longitude, stationObject.latitude, 0, stationObject.azimuthNom);
		dataSource.defaultInsertTranslation(stationObject.station, gdm);
	}
	
	/**
	 * this is where the program lives most of the time, except for startup
	 */
	public void startStreaming() {
		
		try {
			
			// default the connection to the station
			LilyIPConnection connection = null;
			
			// get the latest data time from the tilt database
			Date lastDataTime = dataSource.defaultGetLastDataTime(stationObject.station);
			if (lastDataTime == null) {
				lastDataTime = new Date(0);
			}
			
			// notify what we are trying to poll for
			output("#### " + currentTime.nowString() + " [Start Streaming " + stationObject.station + "] (lastDataTime: " + dateOut.format(lastDataTime) + ")", LOW, true);
			
			// instantiate the connection out of the observatory
			if (connection == null) {
				connection = new LilyIPConnection(deviceIP, devicePort);
			}
			
			boolean reconnect = true;
			while (true) {
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
				if (!reconnect) {
					String msg = connection.getMsg(dataTimeout);
					if (msg != null) {
						try {
							// remove the $ and new line character from the message (lily specific)
							msg = msg.substring(1, msg.length() - 1).trim();
							DataLine dl = new DataLine(stationObject.dataTypeList, stationObject.delimiter, msg);
							DoubleMatrix2D dm	= DoubleFactory2D.dense.make(1, 8);
							dm.setQuick(0, 0, dl.t);
							dm.setQuick(0, 1, dl.x);
							dm.setQuick(0, 2, dl.y);
							dm.setQuick(0, 3, dl.h);
							dm.setQuick(0, 4, dl.b);
							dm.setQuick(0, 5, dl.i);
							dm.setQuick(0, 6, dl.g);
							dm.setQuick(0, 7, dl.r);
							String[] columnNames = {"j2ksec", "xTilt", "yTilt", "holtTemp", "boxTemp", "instVolt", "gndVolt", "rain"};
							GenericDataMatrix gdm = new GenericDataMatrix(dm);
							gdm.setColumnNames(columnNames);
							dataSource.insertData(stationObject.station, gdm, 0);
							dataSource.insertV2Data(stationObject.station.toLowerCase(), dl.t, dl.x, dl.y, dl.h, dl.b, dl.i, dl.g, dl.r);
							output(msg, LOW, true);
						} catch (Exception e) {
							connection.close();
							e.printStackTrace();
							System.out.println("There was an exception, continuing...");
							reconnect = true;
						}
					}
				}	
				// Thread.sleep(betweenPollDelay);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/** 
	 * converts a config file into a log file
	 * 
	 * @param configFile
	 */
	public static String getLogFile (String configFile) {
		String fileName		= null;
		String separator	= File.separator;
		int pos1			= configFile.lastIndexOf(separator);
		fileName			= configFile.substring(pos1 + 1);
		// fileName.replace("config", "log");
		fileName			= "logs/" + fileName + ".log";
		return fileName;
	}

	/** 
	 * outputs a message to the log file.  all output for the program goes through here
	 *
	 */
	public void output(String msg, int msgLevel, boolean newLine) {
		if (msgLevel >= outputLevel) {
			try	{
				PrintWriter out = new PrintWriter(new FileWriter(outputFile, true));
				if (newLine) {
					out.println(msg);
				} else {
					out.print(msg);
				}
				out.close();			
			} catch (Exception e) {
				e.printStackTrace();	
			}
		}
	}	

	/** 
	 * Main method
	 * Command line syntax:
	 * -c configFileName
	 * -h help mode.  print help message
	 * -t test mode.  parse config file and create tables and translations, but do not poll
	 * @param args
	 */
	public static void main(String[] as) {

		boolean doStream = true;
		String cf = null;
		Set<String> flags;
		Set<String> keys;
		
		flags	= new HashSet<String>();
		keys	= new HashSet<String>();
		flags.add("-h");
		flags.add("-t");
		keys.add("-c");
		
		Arguments args = new Arguments(as, flags, keys);
		
		if (args.flagged("-h")) {
			System.err.println("java gov.usgs.vdx.in.DataStreamer -c configFile [-h] [-t]");
			System.exit(-1);
		}
		
		if (args.flagged("-t")) {
			doStream = false;
		}
		
		if (!args.contains("-c")) {
			System.err.println("java gov.usgs.vdx.in.DataStreamer -c configFile [-h] [-t]");
			System.exit(-1);
		}
		
		cf = args.get("-c");

		DataStreamer dataStreamer = new DataStreamer(cf, doStream);
	}
}
