package gov.usgs.vdx.in;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.util.CurrentTime;
import gov.usgs.vdx.db.VDXDatabase;
import gov.usgs.vdx.data.tilt.SQLTiltDataSource;
import gov.usgs.vdx.data.generic.fixed.SQLGenericFixedDataSource;
import gov.usgs.vdx.data.GenericDataMatrix;

import cern.colt.matrix.*;

/**
 * DataPoller.  A class for polling a station through a radio, querying for a data message,
 * parsing the message and importing into the database.
 * @author Loren Antolik
 */
public class DataPoller extends Poller {
	
	// configFile: main data poller config file
	private ConfigFile configFile;
	// configFileName: full path and name of VDX config file
	private String vdxConfig;
	// vdxNameTilt: tilt database prefix
	private String vdxNameTilt;
	// vdxNameStrain: strain database prefix
	private String vdxNameStrain;
	// vdxNameGas: gas database prefix
	private String vdxNameGas;
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
	private int betweenPollDelay;
	// deviceIP: the IP address of the repeater radio
	private String deviceIP;
	// devicePort: the port of the repeater radio used
	private int devicePort;
	
	// stations: a list of the station names
	private List<String> stations;
	// stationIterator: a data structure to parse through the stations in the config file
	private Iterator<String> stationIterator;
	// stationObject: an object which represents a station - parameters and data types
	private Station stationObject;
	// stationList: a list holding all the station objects 
	private List<Station> stationList;
	
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
	// strain translation variables: used to create an entry in the translations table
	private double cs1, ds1, cs2, ds2, cbar, dbar;
	// gas translation variables: used to create an entry in the translations table
	private double cco2l, dco2l, cco2h, dco2h;
	// callNumber: call number of the radio the device is attached to
	private int callNumber;
	// repeater: the id of the repeater to go through
	private int repeater;
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
	// dataLines: how many lines of data to request from the device
	private int dataLines;
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
	private SQLTiltDataSource tiltDataSource;
	private SQLGenericFixedDataSource strainDataSource;
	private SQLGenericFixedDataSource gasDataSource;
	
	// default the connection to the station/
	private FreewaveIPConnection connection;
	
	// NONE: no logging will take place
	private static final int NONE = 0;
	// LOW: normal program operation logging
	private static final int LOW = 1;
	// FULL: debugging logging
	private static final int FULL = 2;
	
	/** 
	 * default constructor
	 */
	public DataPoller () {
		super();
		nextInterval = 0;
		dateIn.setTimeZone(TimeZone.getTimeZone("GMT"));
		dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	/** 
	 * constructor that is used by main
	 * 
	 * @param cf	config file name
	 * @param doPoll	flag to poll the stations
	 */
	public DataPoller (String cf, boolean doPoll) {
		
		this();
		processConfigFile(cf);
		preparePolling();
		
		// if (doPoll) {
			setName("DataPoller [" + configFile.getName() + "]");
			startPolling();
		// }
		
		// System.exit(0);
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
		vdxNameTilt	= Util.stringToString(configFile.getString("vdxNameTilt"), "");
		output("vdxNameTilt:" + vdxNameTilt, FULL, false);
		vdxNameStrain	= Util.stringToString(configFile.getString("vdxNameStrain"), "");
		output(" vdxNameStrain:" + vdxNameStrain, FULL, false);
		vdxNameGas	= Util.stringToString(configFile.getString("vdxNameGas"), "");
		output(" vdxNameGas:" + vdxNameGas, FULL, true);
		output("", FULL, true);

		// output the connection settings
		output("#### Connection Settings ####", FULL, true);
		deviceIP			= Util.stringToString(configFile.getString("deviceIP"), "0.0.0.0");
		output("deviceIP:" + deviceIP, FULL, false);		
		devicePort			= Util.stringToInt(configFile.getString("devicePort"), 0);
		output(" devicePort:" + String.valueOf(devicePort), FULL, false);
		postConnectDelay	= Util.stringToInt(configFile.getString("postConnectDelay"), 5000);
		output(" postConnectDelay:" + String.valueOf(postConnectDelay) + "ms", FULL, false);		
		betweenPollDelay	= Util.stringToInt(configFile.getString("betweenPollDelay"), 5000);
		output(" betweenPollDelay:" + String.valueOf(betweenPollDelay) + "ms", FULL, true);
		output("", FULL, true);
		
		// get the list of all the stations to poll, create an iterator to parse through the sub configs and save them in a list
		stations			= configFile.getList("station");	
		stationIterator		= stations.iterator();
		stationList			= new ArrayList<Station>();
		output("#### Station Settings", FULL, true);
		
		// iterate through each of the stations in the config file
		while (stationIterator.hasNext()) {
			
			// iterate to the next station
			station = stationIterator.next();
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
			
			// strain translations, offsets and defaults
			cs1		= Util.stringToDouble(stationParams.getString("cs1"), 1.0);
			output("cs1:" + String.valueOf(cs1), FULL, false);
			ds1		= Util.stringToDouble(stationParams.getString("ds1"), 0.0);
			output(" ds1:" + String.valueOf(ds1), FULL, false);
			cs2		= Util.stringToDouble(stationParams.getString("cs2"), 1.0);
			output(" cs1:" + String.valueOf(cs2), FULL, false);
			ds2		= Util.stringToDouble(stationParams.getString("ds2"), 0.0);
			output(" ds2:" + String.valueOf(ds2), FULL, false);
			cbar	= Util.stringToDouble(stationParams.getString("cbar"), 1.0);
			output(" cbar:" + String.valueOf(cbar), FULL, false);
			dbar	= Util.stringToDouble(stationParams.getString("dbar"), 0.0);
			output(" dbar:" + String.valueOf(dbar), FULL, true);
			
			// gas translations, offsets and defaults
			cco2l	= Util.stringToDouble(stationParams.getString("cco2l"), 1.0);
			output("cco2l:" + String.valueOf(cco2l), FULL, false);
			dco2l	= Util.stringToDouble(stationParams.getString("dco2l"), 0.0);
			output(" dco2l:" + String.valueOf(dco2l), FULL, false);
			cco2h	= Util.stringToDouble(stationParams.getString("cco2h"), 1.0);
			output(" cco2h:" + String.valueOf(cco2h), FULL, false);
			dco2h= Util.stringToDouble(stationParams.getString("dco2h"), 0.0);
			output(" dco2h:" + String.valueOf(dco2h), FULL, true);
			
			// connection settings
			callNumber		= Util.stringToInt(stationParams.getString("callNumber"));
			output("callNumber:" + String.valueOf(callNumber), FULL, false);			
			repeater			= Util.stringToInt(stationParams.getString("repeater"), 0);
			output(" repeater:" + String.valueOf(repeater), FULL, false);		
			connTimeout		= Util.stringToInt(stationParams.getString("connTimeout"), 50000);
			output(" connTimeout:" + String.valueOf(connTimeout) + "ms", FULL, false);			
			dataTimeout		= Util.stringToInt(stationParams.getString("dataTimeout"), 50000);
			output(" dataTimeout:" + String.valueOf(dataTimeout) + "ms", FULL, false);			
			maxRetries		= Util.stringToInt(stationParams.getString("maxRetries"), 2);
			output(" maxRetries:" + String.valueOf(maxRetries), FULL, false);			
			timeSource		= Util.stringToString(stationParams.getString("timeSource"), "tilt");
			output(" timeSource:" + timeSource, FULL, false);			
			syncInterval	= Util.stringToInt(stationParams.getString("syncInterval"), 0);
			output(" syncInterval:" + syncInterval, FULL, true);			
			dataLines		= Util.stringToInt(stationParams.getString("dataLines"), 50);
			output("dataLines:" + String.valueOf(dataLines), FULL, false);			
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
			                          cs1, ds1, cs2, ds2, cbar, dbar, cco2l, dco2l, cco2h, dco2h, 
			                          callNumber, repeater, connTimeout, dataTimeout, maxRetries, timeSource, syncInterval,
			                          dataLines, instrument, delimiter, dataTypeList);
			
			// place the object into the station array
			stationList.add(stationObject);
			
			// output a blank line for the log file
			output("", FULL, true);
		}
	}
	
	/** 
	 * iterates through each of the stations in the config file and sets up channels and translations if they do not exist
	 * and defaults the channels table to these translations
	 */
	public void preparePolling() {
		
		// iterate through each of the stations, and create
		for (int i = 0; i < stationList.size(); i++) {
		
			// get all configuration settings related to this station
			stationObject	= stationList.get(i);
		
			// initialize the data sources based on instruments at the station
			if (stationObject.hasTilt) {
				vdxParams.put("vdx.name", vdxNameTilt, false);
				tiltDataSource = new SQLTiltDataSource();
				// TODO: work out new intitialization
				// tiltDataSource.initialize(vdxParams);
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
				tiltDataSource.createChannel(stationObject.station, stationObject.name, 
						stationObject.longitude, stationObject.latitude, 0, stationObject.azimuthNom);
				tiltDataSource.defaultInsertTranslation(stationObject.station, gdm);
			}
			if (stationObject.hasStrain) {
				vdxParams.put("vdx.name", vdxNameStrain, false);
				strainDataSource = new SQLGenericFixedDataSource();
				// TODO: work out new initialization
				// strainDataSource.initialize(vdxParams);
				DoubleMatrix2D dm	= DoubleFactory2D.dense.make(1, 6);
				dm.setQuick(0, 0, stationObject.cs1);
				dm.setQuick(0, 1, stationObject.ds1);
				dm.setQuick(0, 2, stationObject.cs2);
				dm.setQuick(0, 3, stationObject.ds2);
				dm.setQuick(0, 4, stationObject.cbar);
				dm.setQuick(0, 5, stationObject.dbar);
				GenericDataMatrix gdm = new GenericDataMatrix(dm);
				gdm.setColumnNames(new String[] {"cstrain1", "dstrain1", "cstrain2", "dstrain2", "cbar", "dbar"});
				strainDataSource.createChannel(stationObject.station, stationObject.name, stationObject.longitude, stationObject.latitude, 0);
				strainDataSource.insertTranslation(stationObject.station, gdm);
			}
			if (stationObject.hasGas) {
				vdxParams.put("vdx.name", vdxNameGas, false);
				gasDataSource = new SQLGenericFixedDataSource();
				// TODO: work out new initialization
				// gasDataSource.initialize(vdxParams);
				DoubleMatrix2D dm	= DoubleFactory2D.dense.make(1, 4);
				dm.setQuick(0, 0, stationObject.cco2l);
				dm.setQuick(0, 1, stationObject.dco2l);
				dm.setQuick(0, 2, stationObject.cco2h);
				dm.setQuick(0, 3, stationObject.dco2h);
				GenericDataMatrix gdm = new GenericDataMatrix(dm);
				gdm.setColumnNames(new String[] {"cco2l", "dco2l", "cco2h", "dco2h"});
				gasDataSource.createChannel(stationObject.station, stationObject.name, stationObject.longitude, stationObject.latitude, 0);
				gasDataSource.insertTranslation(stationObject.station, gdm);
			}
		}
	}
	
	/** 
	 * this is where the program lives most of the time, except for startup
	 */	
	public void poll() {
		
		try {
			
			// begin message
			output("#### " + currentTime.nowString() + " [Start Polling Cycle]\n", LOW, true);
			
			// iterate through this loop for each of the stations
			for (int i = 0; i < stationList.size(); i++) {
				
				// get all configuration settings related to this station
				stationObject	= stationList.get(i);
				
				// get the latest data time from the tilt database
				Date lastDataTime = tiltDataSource.defaultGetLastDataTime(stationObject.station);
				if (lastDataTime == null) {
					lastDataTime = new Date(0);
				}
				
				// default some variables used in the loop
				int tries = 0;
				boolean done = false;
				
				// notify what we are trying to poll for
				output("#### " + currentTime.nowString() + " [Start Polling " + stationObject.station + "] (lastDataTime: " + dateOut.format(lastDataTime) + ")", LOW, true);
				
				// iterate through the maximum number of retries as specified in the config file
				while (tries < stationObject.maxRetries && !done) {
					
					// try to create a connection to the station
					try {
						
						// instantiate the connection out of the observatory
						if (connection == null) {
							connection = new FreewaveIPConnection(deviceIP, devicePort, stationObject.dataTimeout);
						}
						
						// connect to the station
						connection.connect(stationObject.repeater, stationObject.callNumber, stationObject.connTimeout);
						Thread.sleep(postConnectDelay);
						CCSAILMessage ccMsg = new CCSAILMessage(stationObject.tiltid);
						
						// build the data request string
						String dataRequest = ccMsg.makeDA(lastDataTime, stationObject.dataLines);
						connection.writeString(dataRequest);
						
						// get the response from the instrument
						String reply = connection.getMsg(stationObject.dataTimeout);
						String replyString = ccMsg.getMsg(reply, true);
						
						// parse the response by lines
						StringTokenizer st = new StringTokenizer(replyString, "\n");
						
						// iterate through each line
						while (st.hasMoreTokens()) {
							
							// if this is a valid message, then parse it
							String line = st.nextToken();
							if (!line.startsWith("EOF") && !line.startsWith("#") && line.length() > 1) {
								
								// parse this data line based on its delimiter
								DataLine dl = new DataLine(stationObject.dataTypeList, stationObject.delimiter, line);
								
								// process the tilt data.  tilt database
								if (stationObject.hasTilt) {
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
									tiltDataSource.insertData(stationObject.station, gdm, 0);
									
									// valve2 hack
									tiltDataSource.insertV2Data(stationObject.station.toLowerCase(), dl.t, dl.x, dl.y, dl.h, dl.b, dl.i, dl.g, dl.r);
								}
								
								// process the strain data.  generic database
								if (stationObject.hasStrain) {
									DoubleMatrix2D dm	= DoubleFactory2D.dense.make(1, 4);
									dm.setQuick(0, 0, dl.t);
									dm.setQuick(0, 1, dl.s1);
									dm.setQuick(0, 2, dl.s2);
									dm.setQuick(0, 3, dl.bar);
									String[] columnNames = {"j2ksec", "s1", "s2", "bar"};
									GenericDataMatrix gdm = new GenericDataMatrix(dm);
									gdm.setColumnNames(columnNames);
									strainDataSource.insertData(stationObject.station, gdm, 0);
									
									// valve2 hack
									strainDataSource.insertV2StrainData(stationObject.station.toLowerCase(), dl.t, dl.s1, dl.s2, dl.g, dl.bar, dl.h, dl.i, dl.r);
								}
								
								// process the gas data.  generic database
								if (stationObject.hasGas) {
									DoubleMatrix2D dm	= DoubleFactory2D.dense.make(1, 3);
									dm.setQuick(0, 0, dl.t);
									dm.setQuick(0, 1, dl.co2l);
									dm.setQuick(0, 2, dl.co2h);
									String[] columnNames = {"j2ksec", "co2l", "co2h"};
									GenericDataMatrix gdm = new GenericDataMatrix(dm);
									gdm.setColumnNames(columnNames);
									gasDataSource.insertData(stationObject.station, gdm, 0);
									
									// valve2 hack
									gasDataSource.insertV2GasData(1, dl.t, dl.co2l);
									gasDataSource.insertV2GasData(2, dl.t, dl.co2h);
								}	
								
								// output the data to the log file if requested
								output(line, LOW, true);
							}
						}
										
						/*if (stationObject.clockSyncInterval != 0) {
							if ((Util.nowJ2K() - dataSource.getLastSyncTime(stationObject.code)) > stationObject.clockSyncInterval) {
								String updateDate = ccMsg.makeTM();
								double j2ksec = Util.nowJ2K();
								output("--- Time synchronization, server time: " + currentTime.nowString() + ", CCSAIL request: " + updateDate, LOW, true);
								connection.writeString(updateDate);
								output("--- Time synchronized", LOW, true);
								dataSource.setLastSyncTime(stationObject.code, j2ksec);	
							}
						}*/
						
						// if we made it here then no exceptions were thrown, and we got the data
						done = true;
						
					// something happened during this iteration, we may try again
					} catch (Exception e) {
						output("#### " + currentTime.nowString() + " [Try " + (tries + 1) + " of " + stationObject.maxRetries + " Failed]", LOW, true);
						e.printStackTrace();
						
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
					output("#### " + currentTime.nowString() + " [Successfully polled " + stationObject.station + "]\n", LOW, true);
				} else {
					output("#### " + currentTime.nowString() + " [Failed to poll " + stationObject.station + "]\n", LOW, true);
				}
				
				// sleep before accessing the next station
				Thread.sleep(betweenPollDelay);
			}
			
			// output that this polling cycle has been completed for all stations in the config file
			output("#### " + currentTime.nowString() + " [End Polling Cycle]\n\n", FULL, true);
			
		// output the exception if one occurred
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

		boolean doPoll = true;
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
			System.err.println("java gov.usgs.vdx.in.DataPoller -c configFile [-h] [-t]");
			System.exit(-1);
		}
		
		if (args.flagged("-t")) {
			doPoll = false;
		}
		
		if (!args.contains("-c")) {
			System.err.println("java gov.usgs.vdx.in.DataPoller -c configFile [-h] [-t]");
			System.exit(-1);
		}
		
		cf = args.get("-c");

		DataPoller DataPoller = new DataPoller(cf, doPoll);
	}
}