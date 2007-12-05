package gov.usgs.vdx.data.zeno;

import java.io.*;
import java.util.*;
import java.text.*;
import gov.usgs.util.Util;
// import gov.usgs.vdx.data.zeno.*;
// import gov.usgs.valve.Util;
// import gov.usgs.valve.data.strain.*;
// import gov.usgs.valve.data.tilt.*;

/**
 * <p>A Pipe for polling Zenos.  
 * 
 * <p>This uses Valve SQL code for interacting with tilt and strain
 * databases and as such the valve-core-[version].jar file will need
 * to be in the classpath for this program to work.
 *
 * <p>ZenoPipe needs to process a configuration file.  Here is an example file
 * with a complete description of the parameters
 *
 * <p>Here is a summary of output: error messages before polling begins and exceptions
 * are printed to standard error.  Standard success/failure messages will always be output to
 * standard out (regardless of the 'outputLevel' parameter in the config file).  If 'outputLevel'
 * is set to 0 then no external file logging is done; 1 logs the standard success/failure messages;
 * and 2 logs all of the output from the Zeno as well (and outputs it to standard out too).
 * 
 * <pre>
 * #
 * # This file tells ZenoPipe what to do.
 * #
 * #	Each non-blank, non-comment (#) line represents a parameter.
 * #		Each parameter is an equals-separated name-value pair, available parameters are:
 * #			interval: number of milliseconds between polls (the amount of time after a polling interval)
 * #			tilt.driver, strain.driver: database driver to use (you shouldn't ever have to change these)
 * #			tilt.url, strain.url: database url to use (you shouldn't ever have to change these)
 * #			dataLines: number of lines of data to request at most
 * #			postConnectDelay: delay between receiving CONNECT message and asking for data (ms)
 * #			betweenPollDelay: delay between polls (ms)
 * #			outputLevel: 0=none; 1=success/failure logs; 2=full logs
 * # 			outputFile: name of output log
 * #			station.[code]: a station (with corresponding code) to poll, see below
 * #
 * #		Each stations has a semicolon-separated list of parameters, they are:
 * #			station ID
 * #			freewave IP address
 * #			freewave port
 * #			repeater entry
 * #			freewave "phone number"
 * #			connection timeout (ms)
 * #			data timeout (ms)
 * #			maximum retries
 * #			clock reset interval (s) --
 * #				the number of seconds since the lastsync time in the database after which
 * #				ZenoPipe will synchronize the Zeno time with the server time (0 == never)
 * #			last data time source -- available types are:
 * #				tilt: gets last data time from tilt database
 * #				strain: gets last data time from strain database
 * #			data to save -- comma-separated list of data types to put in database, available types are:
 * #				tilt: looks for the date, time, tiltX, tiltY, and ground fields in the data format (see below)
 * #				tiltEnv: looks for the date, time, battery, boxTemp, holeTemp, rain, and ground fields
 * #				strain: looks for the strain1, strain2, and ground fields
 * #				strainEnv: looks for the barometer, holeTemp, battery, rain, and ground fields
 * #				co2: looks for the date, time, and co2 field
 * #			data format -- a comma-separated list of fields to expect from the zeno, available types are:
 * #				station: station ID
 * #				date: zeno data date yy/MM/dd format
 * #				time: zeno data time hh:mm:ss format
 * #				battery: a raw battery voltage
 * #				boxTemp: zeno temperature
 * #				tiltX: x-tilt
 * #				tiltY: y-tilt
 * #				co2: CO2
 * #				holeTemp: hole temperature
 * #				ground: ground
 * #				rain: rainfall
 * #				ignore: ignore
 * #
 * 
 * #System wide variables
 * tilt.driver=org.gjt.mm.mysql.Driver
 * tilt.url=jdbc:mysql://db_write.internal.hvo/tilt?user=root&password=cn4u+i1B&autoReconnect=true
 * strain.driver=org.gjt.mm.mysql.Driver
 * strain.url=jdbc:mysql://db_write.internal.hvo/strain?user=datauser&password=datauser&autoReconnect=true
 * 
 * interval=5000
 * dataLines=199
 * postConnectDelay=5000
 * betweenPollDelay=5000
 * timeResetHour=8
 * outputLevel=1
 * 
 * station.UWE=101;192.168.0.252;4001;0;9017614;3000000;5000000;2;tilt;tilt,tiltEnv;station,date,time,battery,boxTemp,tiltX,tiltY,holeTemp,ground,rain
 * station.MLS=203;192.168.0.252;4001;0;9123041;30000;100000;2;tilt;tilt,tiltEnv,strain,strainEnv;station,date,time,battery,boxTemp,tiltX,tiltY,strain1,strain2,holeTemp,ground,barometer,rain
 * station.HEI=108;192.168.0.252;4002;4;9007709;500000;50000;2;tilt;tilt,tiltEnv;station,date,time,battery,boxTemp,tiltX,tiltY,holeTemp,ground,rain
 * station.MKI=109;192.168.0.252;4002;4;9017615;500000;50000;2;tilt;tilt,tiltEnv;station,date,time,battery,boxTemp,tiltX,tiltY,holeTemp,ground,rain
 * </pre>
 *
 * @author Dan Cervelli
 * @version 1.2
 */
public class ZenoPipe extends Pipe
{
	/** this is the date format that the Zeno returns */
	private SimpleDateFormat dateIn = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
	private SimpleDateFormat dateOut = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/** the configuration file */	
	private ConfigFile configFile;
	
	/** the list of stations to poll */
	private Vector stations;
	
	/** fully qualified JDBC driver for strain database communication */
	private String strainDriver;
	/** JDBC url to strain database */
	private String strainURL;
	
	/** fully qualified JDBC driver for tilt database communication */
	private String tiltDriver;
	/** JDBC url to tilt database */
	private String tiltURL;
	
	/** strain database manager */
	private ZenoStrainDataManager strainDataManager;
	/** tilt database manager */
	private ZenoTiltDataManager tiltDataManager;
	
	/** the number of data lines to process */
	private int dataLines = 10;
	
	/** delay (ms) after a connection before beginning communication */
	private long postConnectDelay = 5000;
	
	/** delay (ms) between station polling */
	private long betweenPollDelay = 5000;
	
	/** whether or not to output a log in addition to System.out.
	 *	The name of the log will be '[configfile without '.config'].log' */
	private int outputLevel;
	
	/** the name of the outputFile */
	private String outputFile;
	
	/** output level indicating no external logging */
	private static final int NONE = 0;
	/** output level indicating a low level of external logging */
	private static final int LOW = 1;
	/** output level indicating the full level of external logging */
	private static final int FULL = 2;
	
	/** the IP connection to the Freewave */
	private FreewaveIPConnection connection;

	/** Constructs a ZenoPipe and does basic initialization.
	 * Use this constructor if you want step-by-step control of the ZenoPipe.
	 */	
	public ZenoPipe()
	{
		super();
		nextInterval = 0;
		dateIn.setTimeZone(TimeZone.getTimeZone("GMT"));
		dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	/** Constructs a ZenoPipe, processes the configuration file and immediately starts polling.
	 * This is the constructor used by the main() function.
	 * @param cf the file name of the config file
	 */
	public ZenoPipe(String cf)
	{
		this();
		processConfigFile(cf);
		setName("ZenoPipe [" + configFile.getName() + "]");
		startPolling();		
	}

	/** Process the configuration file.
	 * @param cf the file name of the config file
	 */	
	public void processConfigFile(String cf)
	{
		configFile = new ConfigFile(cf);
		if (configFile.isCorrupt())
		{
			System.err.println("Config file is corrupt.");
			System.exit(1);			
		}
		strainDriver = configFile.get("strain.driver");
		strainURL = configFile.get("strain.url");
		tiltDriver = configFile.get("tilt.driver");
		tiltURL = configFile.get("tilt.url");
		outputLevel = Integer.parseInt(configFile.get("outputLevel"));
		outputFile = configFile.get("outputFile");
		if (outputFile == null)
			outputFile = configFile.getName() + ".log";
		dataLines = Integer.parseInt(configFile.get("dataLines"));
		postConnectDelay = Long.parseLong(configFile.get("postConnectDelay"));
		betweenPollDelay = Long.parseLong(configFile.get("betweenPollDelay"));
		ZenoStrainDataManager.initialize(strainDriver, strainURL);
		ZenoTiltDataManager.initialize(tiltDriver, tiltURL);
		strainDataManager = (ZenoStrainDataManager)ZenoStrainDataManager.getInstance();
		tiltDataManager = (ZenoTiltDataManager)ZenoTiltDataManager.getInstance();
		interval = Integer.parseInt(configFile.get("interval"));

		stations = new Vector();		
		HashMap config = configFile.getConfig();
		Iterator it = config.keySet().iterator();
		while (it.hasNext())
		{
			String key = (String)it.next();
			if (key.startsWith("station."))
			{
				String val = (String)config.get(key);
				Station station = new Station(key.substring(key.indexOf('.') + 1), val);
				stations.add(station);
			}
		}
	}

	/** Outputs a message to System.out.  Also, if logging is enabled, the message is output
	 * to the log.
	 * @param msg the message
	 */	
	public void output(String msg)
	{
		System.out.println(msg);
		if (outputLevel > NONE)
		{
			try
			{
				PrintWriter out = new PrintWriter(new FileWriter(outputFile, true));
				out.println(msg);
				out.close();			
			}
			catch (Exception e)
			{
				e.printStackTrace();	
			}
		}
	}	
	
	/** Polls the stations.
	 */
	public void poll()
	{
		try
		{
			output("[Start polling cycle]\n");
			for (int i = 0; i < stations.size(); i++)
			{
				Station station = (Station)stations.elementAt(i);
				Date date = getLastDataTime(station);
				if (date == null)
					date = new Date(0);
				output("--- Polling " + station.code + " (last data time: " + dateOut.format(date) + ").\n");
				int tries = 0;
				boolean done = false;
				while (tries < station.maxRetries && !done)
				{
					try
					{
						if (connection == null)
							connection = new FreewaveIPConnection(station.host, station.port, station.dataTimeout);
						
						connection.connect(station.repeater, station.phone, station.connTimeout);
						Thread.sleep(postConnectDelay);
						CCSAILMessage ccMsg = new CCSAILMessage(station.sid);
												
						if (station.clockSyncInterval != 0)
						{
							if ((Util.nowJ2K() - getLastSyncTime(station)) > station.clockSyncInterval)
							{
								String req = ccMsg.makeTM();
								double j2ksec = Util.nowJ2K();
								output("--- Time synchronization, server time: " + new Date() + ", CCSAIL request: " + req);
								connection.writeString(req);
								String reply = connection.getMsg(station.dataTimeout);
								output("--- Time synchronized");
								setLastSyncTime(station, j2ksec);	
							}
						}
						
						String req = ccMsg.makeDA(date, dataLines);
						connection.writeString(req);
						
						String reply = connection.getMsg(station.dataTimeout);
						String replyString = ccMsg.getMsg(reply, true);
						StringTokenizer st = new StringTokenizer(replyString, "\n");
						while (st.hasMoreTokens())
						{
							String oneMsg = st.nextToken();
							if (!oneMsg.startsWith("EOF") && !oneMsg.startsWith("#") && oneMsg.length() > 1)
							{
								Message message = new Message(oneMsg, station.dataFormat);
								station.processData(message);
								if (outputLevel == FULL)
									output(oneMsg);
							}
						}
						done = true;
					}
					catch (Exception e) 
					{
						output("--- Try " + (tries + 1) + " of " + station.maxRetries + " failed.\n");
					}
					finally
					{
						if (connection != null)
							connection.disconnect();
					}
					tries++;
				}
				if (done)
					output("\n--- Successfully polled " + station.code + ".\n");
				else
					output("\n*** Failed to poll " + station.code + ".\n");
				Thread.sleep(betweenPollDelay);
			}
			output("[End polling cycle]\n");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/** Application entry point.  Immediately checks for the existence of a config file
	 * argument; if none exists it quits, otherwise it launches a ZenoPipe.
	 * @param args command line arguments
	 */
	public static void main(String[] args)
	{
		String cf = null;
		if (args.length != 1)
		{
			System.err.println("Usage: java gov.usgs.pipe.zeno.ZenoPipe <configfile>");
			System.err.println();
			System.err.println("Example: java gov.usgs.pipe.zeno.ZenoPipe port4001.config");
			System.exit(1);
		}
		else
			cf = args[0];
			
		ZenoPipe zenoPipe = new ZenoPipe(cf);
	}

	/** Gets the last data time for a station.
	 * @param station the station
	 * @return the last data time
	 */	
	private java.util.Date getLastDataTime(Station station)
	{
		if (station.timeSource.equals("tilt"))
			return tiltDataManager.getLastDataTime(station.code);
		else if (station.timeSource.equals("strain"))
			return strainDataManager.getLastDataTime(station.code);
		else
			return null;
	}
	
	/** Gets the last clock synchronization time.
	 * @param station the station code
	 * @return the last synchronization time (j2ksec)
	 */
	private double getLastSyncTime(Station station)
	{
		if (station.timeSource.equals("tilt"))
			return tiltDataManager.getLastSyncTime(station.code);
		else if (station.timeSource.equals("strain"))
			return strainDataManager.getLastSyncTime(station.code);
		else
			return Double.NaN;
	}
	
	/** Sets the last clock synchronization time.
	 * @param station the station code
	 * @param j2ksec the synchronization time (j2ksec)
	 */
	private void setLastSyncTime(Station station, double j2ksec)
	{
		if (station.timeSource.equals("tilt"))
			tiltDataManager.setLastSyncTime(station.code, j2ksec);
		else if (station.timeSource.equals("strain"))
			strainDataManager.setLastSyncTime(station.code, j2ksec);
	}
	
	/** Class for holding information about a station and parsing the information returned by
	 * the Zeno.
	 */
	class Station
	{
		/** station code */
		String code;
		/** station id */
		int sid;
		/** IP address */
		String host;
		/** IP port */
		int port;
		/** repeater entry number */
		int repeater;
		/** phone number */
		int phone;
		/** connection timeout (ms) */
		int connTimeout;
		/** data timeout (ms) */
		int dataTimeout;
		/** maximum number of retries */
		int maxRetries;
		/** the clock sync interval (s) */
		int clockSyncInterval;
		/** where to get the last data time ('strain' or 'tilt' -- read from config file) */
		String timeSource;
		/** the sets of data that will be saved from this output stream */
		String output;
		/** the comma separated list of outputs to expect from the Zeno */
		String dataFormat;
		
		/** Creates a new Station based on a station entry in the config file.
		 * @param c the station code
		 * @param s the station entry from the config file
		 */
		public Station(String c, String s)
		{
			code = c;
			StringTokenizer st = new StringTokenizer(s, ";");
			sid = Integer.parseInt(st.nextToken());
			host = st.nextToken();
			port = Integer.parseInt(st.nextToken());
			repeater = Integer.parseInt(st.nextToken());
			phone = Integer.parseInt(st.nextToken());
			connTimeout = Integer.parseInt(st.nextToken());
			dataTimeout = Integer.parseInt(st.nextToken());
			maxRetries = Integer.parseInt(st.nextToken());
			clockSyncInterval = Integer.parseInt(st.nextToken());
			timeSource = st.nextToken();
			output = st.nextToken();
			dataFormat = st.nextToken();
		}
		
		/** Process a message. This iterates through each type of output this Station is expected to 
		 * generate as specified in the output section of the station entry and extracts the appropriate
		 * data from the message and finally puts it in the database.
		 * @param msg the message
		 * @return whether or not the message was successfully processed
		 */
		public boolean processData(Message msg)
		{
			try
			{
				//System.out.println("processData: " + output);
				StringTokenizer st = new StringTokenizer(output, ",");
				while (st.hasMoreTokens())
				{
					String outputType = st.nextToken();
					//System.out.println(outputType);
					String ds = msg.get("date") + " " + msg.get("time");

					Date date = dateIn.parse(ds);
					double j2ksec = Util.dateToJ2K(date);
					String dateString = dateOut.format(date);
					if (outputType.equals("tilt"))
					{
						double x = Double.parseDouble(msg.get("tiltX"));
						double y = Double.parseDouble(msg.get("tiltY"));
						double agnd = Double.parseDouble(msg.get("ground"));
						//System.out.println(j2ksec + " " + dateString + " " + x + " " + y + " " + agnd);
						tiltDataManager.insertTiltData(code, j2ksec, dateString, x, y, agnd);
						
						/*
						// I think Peter added this for some debugging.
						PrintWriter out = new PrintWriter(new FileWriter(code + ".log", true));
						out.println(ds + "," + date + "," + Math.round(j2ksec) + "," + dateString);
						out.close();
						*/
					}
					else if (outputType.equals("tiltEnv"))
					{
						double ht = Double.parseDouble(msg.get("holeTemp"));
						double bt = ht;
						try
						{
							bt = Double.parseDouble(msg.get("boxTemp"));
						}
						catch (Exception e)
						{
							System.out.println("bad boxtemp");
							//e.printStackTrace();
						}
						double v = Double.parseDouble(msg.get("battery"));
						double r = Double.parseDouble(msg.get("rain"));
						double agnd = Double.parseDouble(msg.get("ground"));
						//System.out.println(j2ksec + " " + dateString + " " + ht + " " + bt + " " + v + " " + r + " " + agnd);
						tiltDataManager.insertTiltEnvData(code, j2ksec, dateString, ht, bt, v, r, agnd);
					}
					else if (outputType.equals("co2"))
					{
						double co2 = Double.parseDouble(msg.get("co2"));
						//System.out.println(j2ksec + " " + dateString + " " + co2);
						GasDataManager.getDataManager().addCO2Data(j2ksec, 1, dateString, co2);
					}
					else if (outputType.equals("co2high"))
                                        {
                                                double co2high = Double.parseDouble(msg.get("co2high"));
                                                System.out.println("test " + j2ksec + " " + dateString + " " + co2high);
                                                GasDataManager.getDataManager().addCO2Data(j2ksec, 2, dateString, co2high);
                                        }

					else if (outputType.equals("strain"))
					{
						double dt01 = Double.parseDouble(msg.get("strain1"));
						double dt02 = Double.parseDouble(msg.get("strain2"));
						double agnd = Double.parseDouble(msg.get("ground"));
						//System.out.println(j2ksec + " " + dateString + " " + dt01 + " " + dt02 + " " + agnd);
						strainDataManager.insertStrainData(code, j2ksec, dateString, dt01, dt02, agnd);
					}
					else if (outputType.equals("strainEnv"))
					{
						double bar = Double.parseDouble(msg.get("barometer"));
						double ht = Double.parseDouble(msg.get("holeTemp"));
						double v = Double.parseDouble(msg.get("battery"));
						double r = Double.parseDouble(msg.get("rain"));
						double agnd = Double.parseDouble(msg.get("ground"));
						//System.out.println(j2ksec + " " + dateString + " " + bar + " " + ht + " " + " " + v + " " + r + " " + agnd);
						strainDataManager.insertStrainEnvData(code, j2ksec, dateString, bar, ht, v, r, agnd);
					}
				}
				return true;
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			return false;
		}
	}
	
	
	/** Utility class for parsing messages from the ZenoPipe against an expected data format.
	 * Essentially, the ZenoPipe returns a comma-separated list of data, and this applies a map
	 * of natural language names to each data element.
	 */
	class Message
	{
		/** the data fields in the message */
		HashMap fields;
		
		/** Creates a Message with fields named according to the specified format.
		 * @param msg the message
		 * @param format the format
		 */
		public Message(String msg, String format)
		{
			fields = new HashMap();
			Vector f = new Vector();
			StringTokenizer mst = new StringTokenizer(msg, ",");
			StringTokenizer fst = new StringTokenizer(format, ",");
			while (fst.hasMoreTokens())
			{
				String key = fst.nextToken();
				String val = mst.nextToken();
				if (!key.equals("ignore"))
					fields.put(key, val);
			}
		}
		
		/** Get a field from the message.
		 * @param key the name of the field
		 * @return the field
		 */
		public String get(String key)
		{
			return (String)fields.get(key);
		}
	}
}