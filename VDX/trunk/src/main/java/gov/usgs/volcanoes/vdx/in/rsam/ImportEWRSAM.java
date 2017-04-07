package gov.usgs.volcanoes.vdx.in.rsam;

import gov.usgs.earthworm.ImportGeneric;
import gov.usgs.earthworm.MessageListener;
import gov.usgs.earthworm.message.Message;
import gov.usgs.earthworm.message.MessageType;
import gov.usgs.earthworm.message.TraceBuf;
import gov.usgs.plot.data.GenericDataMatrix;
import gov.usgs.util.CodeTimer;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.CurrentTime;
import gov.usgs.util.Log;
import gov.usgs.util.Util;
import gov.usgs.volcanoes.vdx.data.Channel;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceHandler;
import gov.usgs.volcanoes.vdx.data.rsam.SQLRSAMDataSource;
import gov.usgs.winston.in.ew.ChannelStatus;
import gov.usgs.winston.in.ew.Options;
import gov.usgs.winston.in.ew.OptionsFilter;
import gov.usgs.winston.in.ew.TraceBufFilter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/**
 * The new ImportEWRSAM.
 * 
 * @author Dan Cervelli
 * @author Richard Guest
 * @author Tom Parker
 * @author Loren Antolik
 */
public class ImportEWRSAM extends Thread {

	public static final String DEFAULT_CONFIG_FILENAME = "ImportEWRSAM.config";
	public static final String DEFAULT_HOST = "localhost";
	public static final int DEFAULT_PORT = 16022;
	// public static final String DEFAULT_EXPORT_TYPE = "old";
	public static final int DEFAULT_TIMEOUT = 2000;
	public static final int DEFAULT_HEARTBEAT_INTERVAL = 30000;
	public static final int DEFAULT_EXPECTED_HEARTBEAT_INTERVAL = 30000;
	public static final int DEFAULT_STATUS_INTERVAL = 60;
	public static final String DEFAULT_RECEIVE_ID = "MSG_FROM_EXPORT";
	public static final String DEFAULT_SEND_ID = "MSG_TO_EXPORT";
	public static final String DEFAULT_DRIVER = "com.mysql.jdbc.Driver";
	public static final String DEFAULT_TABLE_ENGINE = "MyISAM";
	public static final int DEFAULT_MAX_DAYS = 0;
	public static final int DEFAULT_MAX_BACKLOG = 100;
	public static final String DEFAULT_LOG_LEVEL = "WARNING";
	public static final String DEFAULT_LOG_FILE = "ImportEWRSAM.log";
	public static final int DEFAULT_LOG_NUM_FILES = 10;
	public static final int DEFAULT_LOG_FILE_SIZE = 1000000;

	public static final double DEFAULT_TIME_THRESHOLD = 1.0;
	public static final int DEFAULT_BACKLOG_THRESHOLD = 1;
	public static final boolean DEFAULT_RSAM_ENABLE = true;
	public static final int DEFAULT_RSAM_DELTA = 10;
	public static final int DEFAULT_RSAM_DURATION = 60;

	public static final int DEFAULT_DROP_TABLE_DELAY = 10;

	public static final int DEFAULT_REPAIR_RETRY_INTERVAL = 10 * 60;

	// JSAP related stuff.
	public static String JSAP_PROGRAM_NAME = "java gov.usgs.volcanoes.vdx.in.rsam.ImportEWRSAM";
	public static String JSAP_EXPLANATION_PREFACE = "ImportEWRSAM\n"
			+ "\n"
			+ "This program gets data from an Earthworm export process and imports\n"
			+ "it into a Valve database. See 'ImportEWRSAM.config' for more options.\n"
			+ "\n";

	private static final String DEFAULT_JSAP_EXPLANATION = "All output goes to both standard error and the file log.\n"
			+ "\n"
			+ "While the process is running (and accepting console input) you can enter\n"
			+ "these commands into the console (followed by [Enter]):\n"
			+ "0: turn logging off.\n"
			+ "1: normal logging level (WARNING).\n"
			+ "2: high logging level (FINE).\n"
			+ "3: log everything.\n"
			+ "s: display status information.\n"
			+ "c[col][-]: channel list, sorted by col, - for descending. Examples: c, cl-, cx\n"
			+ "i: no longer accept console input.\n"
			+ "q: quit cleanly.\n"
			+ "ctrl-c: quit now.\n"
			+ "\n"
			+ "Note that if console input is disabled the only way to\n"
			+ "terminate the program is with ctrl-c or by killing the process.\n";

	private static final Parameter[] DEFAULT_JSAP_PARAMETERS = new Parameter[] {
			new FlaggedOption(
					"logLevel",
					JSAP.STRING_PARSER,
					JSAP.NO_DEFAULT,
					JSAP.NOT_REQUIRED,
					'l',
					"log-level",
					"The level of logging to start with\n"
							+ "This may consist of either a java.util.logging.Level name or an integer value.\n"
							+ "For example: \"SEVERE\", or \"1000\""),
			new Switch("logoff", '0', "logoff",
					"Turn logging off (equivalent to --log-level OFF)."),
			new Switch("lognormal", '1', "lognormal",
					"Normal (default) logging level (equivalent to --log-level WARNING)."),
			new Switch("loghigh", '2', "loghigh",
					"High logging level (equivalent to --log-level FINE)."),
			new Switch("logall", '3', "logall",
					"High logging level (equivalent to --log-level ALL)."),
			new Switch("noinput", 'i', "noinput",
					"Don't accept input from the console."),
			new UnflaggedOption("configFilename", JSAP.STRING_PARSER,
					JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY,
					"The config file name.") };

	protected String configFilename;
	protected ConfigFile config;	

	public SQLRSAMDataSource sqlDataSource;
	public SQLDataSourceHandler sqlDataSourceHandler;
	public SQLDataSourceDescriptor sqlDataSourceDescriptor;

	protected ImportGeneric importGeneric;
	private final Set<String> existingChannels;
	private final Map<String, ConcurrentLinkedQueue<TraceBuf>> channelTraceBufs;

	protected final Logger logger;
	protected String logFile;
	protected int logNumFiles;
	protected int logSize;

	protected final CodeTimer inputTimer;
	protected int totalTraceBufsWritten = 0;
	protected int totalTraceBufs;
	protected int totalTraceBufsDropped;
	protected int totalTraceBufsAccepted;
	protected int totalTraceBufsRejected;
	protected int totalTraceBufsFailed;
	protected final Map<String, ChannelStatus> channelStatus;
	protected final Date importStartTime;
	protected final DateFormat dateFormat;
	protected final DateFormat vdxDateFormat;

	protected int dropTableDelay = 10000;
	protected boolean enableValarmView;

	protected Options defaultOptions;
	protected final Map<String, Options> channelOptions;
	protected Map<String, Map<String, String>> channelMetadata;

	private volatile boolean quit = false;

	protected List<TraceBufFilter> traceBufFilters;
	protected List<OptionsFilter> optionFilters;

	public ImportEWRSAM(String fn) {
		this();

		configFilename = Util.stringToString(fn, DEFAULT_CONFIG_FILENAME);

		importGeneric = new ImportGeneric() {
			public void outOfMemoryErrorOccurred(OutOfMemoryError e) {
				handleOutOfMemoryError(e);
			}
		};
		importGeneric.setLogger(logger);

		config = new ConfigFile(configFilename);

		channelMetadata = new HashMap<String, Map<String, String>>();

		processConfigFile();
	}

	/**
	 * Sets up all the standard private member variables, the logger and
	 * instantiates a ConfigFile using configFilename.
	 */
	public ImportEWRSAM() {
		setName("ImportEWRSAM");

		importStartTime = CurrentTime.getInstance().nowDate();
		dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		vdxDateFormat = new SimpleDateFormat("yyyy_MM_dd");
		vdxDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		channelOptions = new HashMap<String, Options>();
		channelStatus = new HashMap<String, ChannelStatus>();

		inputTimer = new CodeTimer("inputTimer", false);
		channelTraceBufs = new ConcurrentHashMap<String, ConcurrentLinkedQueue<TraceBuf>>(
				200, 0.75f, 1);
		existingChannels = Collections.synchronizedSet(new HashSet<String>());

		logger = Log.getLogger("gov.usgs.volcanoes.vdx");
		logger.setLevel(Level.parse(DEFAULT_LOG_LEVEL));
	}

	/**
	 * Try to recover from an {@link OutOfMemoryError} by dropping all
	 * {@link TraceBuf}s from the processing queue. Ideally,
	 * {@link OutOfMemoryError}s should be prevented by aggressive tuning; this
	 * is a last-gasp effort to allow the program to continue running.
	 */
	public void handleOutOfMemoryError(OutOfMemoryError e) {
		channelTraceBufs.clear();
		logger.warning("Handled OutOfMemoryError, TraceBuf queues cleared.");
		e.printStackTrace();
	}

	/**
	 * Logs a severe message and exits uncleanly.
	 */
	protected void fatalError(String msg) {
		logger.severe(msg);
		System.exit(1);
	}

	/**
	 * Processes logger, import and vdx config, plus options, default options
	 * and filters.
	 */
	protected void processConfigFile() {
		processLoggerConfig();
		processImportConfig();
		processVDXConfig();
		processDefaultOptions();
		processOptions();
		processFilters();
	}

	/**
	 * Extracts logging configuration information.
	 */
	protected void processLoggerConfig() {
		logFile = Util.stringToString(config.getString("import.log.name"),
				DEFAULT_LOG_FILE);
		logNumFiles = Util.stringToInt(config.getString("import.log.numFiles"),
				DEFAULT_LOG_NUM_FILES);
		logSize = Util.stringToInt(config.getString("import.log.maxSize"),
				DEFAULT_LOG_FILE_SIZE);

		if (logNumFiles > 0)
			Log.attachFileLogger(logger, logFile, logSize, logNumFiles, true);

		String[] version = Util.getVersion("gov.usgs.volcanoes.vdx");
		if (version != null)
			logger.info("Version: " + version[0] + " Built: " + version[1]);
		else
			logger.info("No version information available.");

		logger.info("config: import.log.name=" + logFile);
		logger.info("config: import.log.numFiles=" + logNumFiles);
		logger.info("config: import.log.maxSize=" + logSize);
	}

	/**
	 * Extracts generalised configuration information.
	 */
	protected void processImportConfig() {
		String host = Util.stringToString(config.getString("import.host"),
				DEFAULT_HOST);
		int port = Util.stringToInt(config.getString("import.port"),
				DEFAULT_PORT);
		// String exportType =
		// Util.stringToString(config.getString("import.exportType"),
		// DEFAULT_EXPORT_TYPE);
		logger.info("config: import.host=" + host);
		logger.info("config: import.port=" + port);
		// logger.info("config: import.exportType=" + exportType);

		// importGeneric.setHostAndPortAndType(host, port, exportType);
		importGeneric.setHostAndPort(host, port);

		String recvID = Util.stringToString(
				config.getString("import.receiveID"), DEFAULT_RECEIVE_ID);
		importGeneric.setRecvIDString(recvID);
		logger.info("config: import.receiveID=" + recvID);
		String sendID = Util.stringToString(config.getString("import.sendID"),
				DEFAULT_SEND_ID);
		importGeneric.setSendIDString(sendID);
		logger.info("config: import.sendID=" + sendID);

		int hbInt = Util.stringToInt(
				config.getString("import.heartbeatInterval"),
				DEFAULT_HEARTBEAT_INTERVAL);
		importGeneric.setHeartbeatInterval(hbInt);
		logger.info("config: import.heartbeatInterval=" + hbInt);

		int ehbInt = Util.stringToInt(
				config.getString("import.expectedHeartbeatInterval"),
				DEFAULT_EXPECTED_HEARTBEAT_INTERVAL);
		importGeneric.setExpectedHeartbeatInterval(ehbInt);
		logger.info("config: import.expectedHeartbeatInterval=" + ehbInt);

		int to = Util.stringToInt(config.getString("import.timeout"),
				DEFAULT_TIMEOUT);
		importGeneric.setTimeout(to);
		logger.info("config: import.timeout=" + to);

		dropTableDelay = Util.stringToInt(
				config.getString("import.dropTableDelay"),
				DEFAULT_DROP_TABLE_DELAY);
		dropTableDelay *= 1000;
		logger.info("config: import.dropTableDelay=" + dropTableDelay);

	}

	/**
	 * Extracts configuration information specific to VDX DB and instantiates a
	 * VDX DB connection.
	 */
	protected void processVDXConfig() {
		String vdxDriver = Util.stringToString(config.getString("vdx.driver"),
				DEFAULT_DRIVER);
		logger.info("config: vdx.driver=" + vdxDriver);

		String vdxPrefix = config.getString("vdx.prefix");
		if (vdxPrefix == null)
			fatalError("vdx.prefix is missing from config file.");
		logger.info("config: vdx.prefix=" + vdxPrefix);

		String vdxURL = config.getString("vdx.url");
		if (vdxURL == null)
			fatalError("vdx.url is missing from config file.");
		logger.info("config: vdx.url=" + vdxURL);

		String vdxName = config.getString("vdx.name");
		if (vdxName == null)
			fatalError("vdx.name is missing from config file.");
		logger.info("config: vdx.name=" + vdxName);
		
		sqlDataSourceHandler	= new SQLDataSourceHandler(vdxDriver, vdxURL, vdxPrefix);
		sqlDataSourceDescriptor	= sqlDataSourceHandler.getDataSourceDescriptor(vdxName);
		if (sqlDataSourceDescriptor == null) {
			logger.log(Level.SEVERE, vdxName + " not in vdxSources.config");
			System.exit(-1);
		}
		sqlDataSource	= (SQLRSAMDataSource) sqlDataSourceDescriptor.getSQLDataSource();
	}

	protected void processDefaultOptions() {
		defaultOptions = new Options();
		defaultOptions.bufThreshold = DEFAULT_BACKLOG_THRESHOLD;
		defaultOptions.timeThreshold = DEFAULT_TIME_THRESHOLD;
		defaultOptions.maxBacklog = DEFAULT_MAX_BACKLOG;
		defaultOptions.maxDays = DEFAULT_MAX_DAYS;
		defaultOptions.rsamEnable = DEFAULT_RSAM_ENABLE;
		defaultOptions.rsamDelta = DEFAULT_RSAM_DELTA;
		defaultOptions.rsamDuration = DEFAULT_RSAM_DURATION;

		ConfigFile dcf = config.getSubConfig("Default");
		defaultOptions = Options.createOptions(dcf, defaultOptions);
		logger.info("config, options, Default: " + defaultOptions);
	}

	protected void processOptions() {
		optionFilters = new ArrayList<OptionsFilter>();
		List<String> optionSets = config.getList("options");
		for (String options : optionSets) {
			if (options.equals("Default"))
				continue;
			ConfigFile tc = config.getSubConfig(options);
			try {
				OptionsFilter filter = new OptionsFilter(options, tc,
						defaultOptions);
				optionFilters.add(filter);
			} catch (Exception ex) {
				logger.log(Level.SEVERE, "Could not create options: ", ex);
			}
		}
		for (OptionsFilter filter : optionFilters)
			logger.info("config, options, " + filter.getName() + ": " + filter);
	}

	protected void processFilters() {
		traceBufFilters = new ArrayList<TraceBufFilter>();
		List<String> filters = config.getList("filter");
		for (String filterName : filters) {
			ConfigFile fc = config.getSubConfig(filterName);
			try {
				TraceBufFilter filter = (TraceBufFilter) Class.forName(
						fc.getString("class")).newInstance();
				filter.configure(fc);
				traceBufFilters.add(filter);
			} catch (Exception ex) {
				logger.log(Level.SEVERE, "Could not create TraceBuf filter: ",
						ex);
			}
		}
		Collections.sort(traceBufFilters);
		for (TraceBufFilter filter : traceBufFilters)
			logger.info("config, filter: " + filter);
	}

	/**
	 * Tests the TraceBuf against each filter, stopping at first match.
	 * 
	 * @param tb
	 *            the TraceBuf to test
	 * @return filter that matched, null if no match
	 */
	protected TraceBufFilter matchFilter(TraceBuf tb) {
		Options options = getOptions(tb);
		for (TraceBufFilter filter : traceBufFilters) {
			if (filter.match(tb, options)) {

				Map<String, String> m = filter.getMetadata();
				if (m != null)
					addMetadata(tb.toWinstonString(), m);

				if (filter.isTerminal())
					return filter;
			}

		}
		return null;
	}

	private void addMetadata(String c, Map<String, String> m) {
		Map<String, String> cm = channelMetadata.get(c);
		if (cm == null) {
			cm = new HashMap<String, String>();
			channelMetadata.put(c, cm);
		}

		cm.putAll(m);
	}

	/**
	 * This class is called by the {@link ImportGeneric} thread whenever a
	 * tracebuf has been received.
	 * 
	 * $Log: not supported by cvs2svn $ Revision 1.11 2007/05/03 23:25:54
	 * dcervelli OOM diagnosis.
	 * 
	 * @author Dan Cervelli
	 */
	class TraceBufHandler implements MessageListener {
		public void messageReceived(Message msg) {
			totalTraceBufs++;
			TraceBuf tb = (TraceBuf) msg;
			logger.finest("RX: " + tb.toString());

			TraceBufFilter matchingFilter = matchFilter(tb);
			boolean accept = false;
			if (matchingFilter != null) {
				accept = matchingFilter.isAccept();

				// Don't consume cycles on String.format if the message is not
				// going
				// to be logged. Yes, it does matter. Running String.format for
				// every
				// single tracebuf adds up.
				if (logger.isLoggable(matchingFilter.getLogLevel()))
					logger.log(
							matchingFilter.getLogLevel(),
							String.format("%s: %s", matchingFilter,
									tb.toString()));
			} else
				logger.finest("No matching filter, rejected: " + tb);

			if (accept) {
				totalTraceBufsAccepted++;
				addTraceBufToQueue(tb);
			} else {
				totalTraceBufsRejected++;

				// ack tracebufs that were rejected, so they will not be resent.
				if (msg.sendAck)
					importGeneric.sendAck(tb.seq);

				if (matchingFilter.keepRejects()) {
					// TODO: deal with the rejected keepers
				}
			}
		}
	}

	protected void addTraceBufToQueue(TraceBuf tb) {
		String channel = tb.toWinstonString();
		ConcurrentLinkedQueue<TraceBuf> q = channelTraceBufs.get(channel);
		if (q == null) {
			q = new ConcurrentLinkedQueue<TraceBuf>();
			channelTraceBufs.put(channel, q);
		}

		Options ip = getOptions(tb);

		q.add(tb);
		while (q.size() > ip.maxBacklog) {
			q.poll();
			// TODO: improve logging of dropped tracebufs
			totalTraceBufsDropped++;
			if (totalTraceBufsDropped % 100 == 1) {
				logger.fine("Overfull backlog, dropped TraceBuf");
			}
		}
	}

	private void cycle(boolean force) {
		// CodeTimer ct0 = new CodeTimer("init");
		for (String key : channelTraceBufs.keySet()) {
			ConcurrentLinkedQueue<TraceBuf> q = channelTraceBufs.get(key);
			if (q.isEmpty())
				continue;

			Options ip = getOptions(q.peek());

			if (force
					|| ip.thresholdExceeded(q.peek().getStartTimeJ2K(),
							q.size())) {
				importChannel(q);
			}
		}
		// ct0.stop();
		// if (ct0.getRunTimeMillis() > 1000)
		// System.out.println("Long cycle: " + ct0);
	}

	private void importChannel(final ConcurrentLinkedQueue<TraceBuf> q) {
		if (q.isEmpty()) {
			System.out.println("isempty: " + q.isEmpty());
			return;
		}
		TraceBuf tb			= q.peek();
		// String code			= tb.toWinstonString();
		String code			= tb.station() + "$" + tb.channel() + "$" + tb.network();

		if (!existingChannels.contains(code) && sqlDataSource.defaultGetChannel(code, sqlDataSource.getChannelTypesFlag()) == null) {
			logger.info("Creating new channel '" + code + "' in VDX database.");
			Channel channel	= new Channel(0, code, code, Double.NaN, Double.NaN, Double.NaN, 1);
			sqlDataSource.defaultCreateChannel(channel, 1, 
					sqlDataSource.getChannelsFlag(), sqlDataSource.getTranslationsFlag(), 
					sqlDataSource.getRanksFlag(), sqlDataSource.getColumnsFlag());
			sqlDataSource.create10MinAvgView(code);
		}
		existingChannels.add(code);

		ArrayList<TraceBuf> tbs = new ArrayList<TraceBuf>(q.size());
		while (!q.isEmpty()) {
			TraceBuf t = q.poll();
			tbs.add(t);
			if (t.sendAck)
				importGeneric.sendAck(t.seq);
		}
		
		Iterator<TraceBuf> it = tbs.iterator();
		while (it.hasNext()) {
			tb	= it.next();
			DoubleMatrix2D dm	= DoubleFactory2D.dense.make(1, 2);
			dm.setQuick(0, 0, tb.getStartTimeJ2K());
			dm.setQuick(0, 1, tb.samples()[0]);
			GenericDataMatrix gdm = new GenericDataMatrix(dm);
			gdm.setColumnNames(new String[] {"j2ksec", "rsam"});
			sqlDataSource.defaultInsertData(code, gdm, sqlDataSource.getTranslationsFlag(), sqlDataSource.getRanksFlag(), 0);
			logger.log(Level.FINE, code + " " + Util.j2KToDateString(tb.getStartTimeJ2K()) + " rsam:" + tb.samples()[0]);
            totalTraceBufsWritten++;
		}
		
		ChannelStatus status = channelStatus.get(code);
		if (status == null) {
			status = new ChannelStatus(code);
			channelStatus.put(code, status);
		}
	}

	public void run() {
		// Register our interest in tracebuf messages.
		TraceBufHandler tbh = new TraceBufHandler();
		importGeneric.addListener(MessageType.TYPE_TRACEBUF, tbh);
		importGeneric.addListener(MessageType.TYPE_TRACEBUF2, tbh);

		boolean connected = false;
		while (!connected) {
			connected = importGeneric.connect();
			if (!connected) {
				try {
					Thread.sleep(100);
				} catch (Exception e) {
				}
			}
		}
		while (!quit) {
			try {
				cycle(true);
				Thread.sleep(10); // avoid busy-waiting when importing few
									// channels
			} catch (OutOfMemoryError e) {
				handleOutOfMemoryError(e);
			} catch (Throwable e) {
				logger.log(Level.SEVERE, "Main loop exception: ", e);
			}
		}
		try {
			cycle(true);
		} catch (Throwable e) {
			logger.log(Level.SEVERE, "Exception during final cycle: ", e);
		}
	}

	protected Options getOptions(TraceBuf tb) {
		String channel = tb.toWinstonString();
		Options ip = channelOptions.get(channel);
		if (ip == null) {
			ip = defaultOptions;
			for (OptionsFilter tf : optionFilters) {
				if (tf.match(tb)) {
					ip = tf.getOptions();
					break;
				}
			}
			channelOptions.put(channel, ip);
		}
		return ip;
	}

	public void setLogLevel(Level level) {
		System.out.println("Logging set to " + level);
		logger.setLevel(level);
	}

	public void quit() {
		importGeneric.shutdown();
		try {
			importGeneric.join();
		} catch (Throwable e) {
			logger.log(Level.SEVERE,
					"Failed to cleanly shutdown SeedLink Collector", e);
		}

		logger.fine("Quitting cleanly.");
		quit = true;
	}

	public void printStatus() {
		Date now = CurrentTime.getInstance().nowDate();
		long nowST = System.currentTimeMillis();
		double uptime = (double) (now.getTime() - importStartTime.getTime()) / 1000.0;
		List<String> strings = new ArrayList<String>(4);

		strings.add("------- ImportEWRSAM --------");
		strings.add("Status time: " + dateFormat.format(now));
		strings.add("Start time:  " + dateFormat.format(importStartTime));
		strings.add("Up time:     " + Util.timeDifferenceToString(uptime));

		long ht = importGeneric.getLastHeartbeatTime();
		double dt = (double) (nowST - ht) / 1000;
		if (ht == 0)
			strings.add("Last HB RX:  (never)");
		else
			strings.add("Last HB RX:  " + dateFormat.format(new Date(ht))
					+ ", " + Util.timeDifferenceToString(dt));
		ht = importGeneric.getLastHeartbeatSentTime();
		dt = (double) (nowST - ht) / 1000;
		if (ht == 0)
			strings.add("Last HB TX:  (never)");
		else
			strings.add("Last HB TX:  " + dateFormat.format(new Date(ht))
					+ ", " + Util.timeDifferenceToString(dt));

		Runtime rt = Runtime.getRuntime();
		strings.add(String.format("Memory (used / max): %.1fMB / %.1fMB",
				(rt.totalMemory() - rt.freeMemory()) / 1024.0 / 1024.0,
				rt.maxMemory() / 1024.0 / 1024.0));

		strings.add("---- TraceBufs");
		strings.add("Accepted: " + totalTraceBufsAccepted);
		strings.add("Written:  " + totalTraceBufsWritten);
		strings.add("Failed:   " + totalTraceBufsFailed);
		strings.add("Rejected: " + totalTraceBufsRejected);
		strings.add("Dropped:  " + totalTraceBufsDropped);
		int pending = totalTraceBufsAccepted - totalTraceBufsWritten
				- totalTraceBufsFailed - totalTraceBufsRejected
				- totalTraceBufsDropped;
		strings.add("Pending:  " + pending);

		// by each filter
		strings.add("---- Timing");
		strings.add(String.format("Total input time:        %s", Util
				.timeDifferenceToString(inputTimer.getTotalTimeMillis() / 1000)));
		strings.add(String.format("Input time per TraceBuf: %.2fms",
				inputTimer.getTotalTimeMillis() / totalTraceBufsWritten));

		for (String s : strings)
			System.out.println(s);
	}

	public void printChannels(String s) {
		char col = 'C';
		if (s.length() > 1)
			col = s.charAt(1);
		boolean desc = s.endsWith("-");

		System.out.println("------- Channels --------");
		System.out.println(ChannelStatus.getHeaderString());
		List<ChannelStatus> channels = new ArrayList<ChannelStatus>(
				channelStatus.size());
		channels.addAll(channelStatus.values());

		Collections.sort(channels, ChannelStatus.getComparator(
				ChannelStatus.SortOrder.parse(col), desc));

		for (ChannelStatus channel : channels) {
			System.out.println(channel.toString());
		}
		System.out.println(ChannelStatus.getHeaderString());
	}

	/**
	 * Find and parse the command line arguments.
	 * 
	 * @param args
	 *            The command line arguments.
	 */
	public static JSAPResult getArguments(String[] args) {
		JSAPResult config = null;
		try {
			SimpleJSAP jsap = new SimpleJSAP(JSAP_PROGRAM_NAME,
					JSAP_EXPLANATION_PREFACE + DEFAULT_JSAP_EXPLANATION,
					DEFAULT_JSAP_PARAMETERS);

			config = jsap.parse(args);

			if (jsap.messagePrinted()) {
				// The following error message is useful for catching the case
				// when args are missing, but help isn't printed.
				if (!config.getBoolean("help")) {
					System.err.println("Try using the --help flag.");
				}
				System.exit(1);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(1);
		}
		return config;
	}

	public static void printKeys() {
		StringBuffer sb = new StringBuffer();
		sb.append("Keys:\n");
		sb.append(" 0-3: logging level\n");
		sb.append("   i: stop accepting console commands\n");
		sb.append("   s: print status\n");
		sb.append("   c: print channels\n");
		sb.append("   q: quit\n");
		sb.append("   ?: display keys\n");

		System.out.println(sb);
	}

	/**
	 * Manage the console input.
	 * 
	 * @param im
	 *            The VDX importer.
	 */
	public static void consoleInputManager(ImportEWRSAM im) {
		im.logger.entering(im.getClass().getName(), "consoleInputManager");
		boolean acceptCommands = true;
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		im.logger.info("Enter ? for console commands.");
		while (acceptCommands) {
			try {
				String s = null;
				try {
					s = in.readLine();
				} catch (IOException ioex) {
					im.logger
							.log(Level.SEVERE,
									"IOException encountered while attempting to read console input.",
									ioex);
				}

				if (s != null) {
					s = s.toLowerCase().trim();
					if (s.equals("q")) {
						im.quit();
						try {
							im.join();
						} catch (Throwable e) {
							im.logger.log(Level.SEVERE,
									"Failed to quit cleanly.", e);
						} finally {
							im.printStatus();
						}
						System.exit(0);
					} else if (s.equals("s"))
						im.printStatus();
					else if (s.startsWith("c"))
						im.printChannels(s);
					else if (s.equals("0"))
						im.setLogLevel(Level.OFF);
					else if (s.equals("1"))
						im.setLogLevel(Level.WARNING);
					else if (s.equals("2"))
						im.setLogLevel(Level.FINE);
					else if (s.equals("3"))
						im.setLogLevel(Level.ALL);
					else if (s.equals("i")) {
						acceptCommands = false;
						im.logger
								.warning("No longer accepting console commands.");
					} else if (s.equals("?"))
						ImportEWRSAM.printKeys();
					else
						ImportEWRSAM.printKeys();
				}
			} catch (OutOfMemoryError e) {
				im.handleOutOfMemoryError(e);
			}
		}
		im.logger.exiting(im.getClass().getName(), "consoleInputManager");
	}

	public static void main(String[] args) {
		JSAPResult config = getArguments(args);

		String fn = Util.stringToString(config.getString("configFilename"),
				DEFAULT_CONFIG_FILENAME);

		Level logLevel = Level.parse(DEFAULT_LOG_LEVEL);

		if (config.getString("logLevel") != null) {
			try {
				logLevel = Level.parse(config.getString("logLevel"));
			} catch (IllegalArgumentException ex) {
				System.err.println("Invalid log level: "
						+ config.getString("logLevel"));
				System.err.println("Using default log level: " + logLevel);
			}
		} else {
			if (config.getBoolean("logoff"))
				logLevel = Level.OFF;

			if (config.getBoolean("lognormal"))
				logLevel = Level.WARNING;

			if (config.getBoolean("loghigh"))
				logLevel = Level.FINE;

			if (config.getBoolean("logall"))
				logLevel = Level.ALL;

		}

		ImportEWRSAM im = new ImportEWRSAM(fn);
		im.setLogLevel(logLevel);

		// Start the importer. start() automatically calls run()
		im.start();

		// Decide if we're accepting commands based on the args parsed by jsap
		// and pass the importer to the consoleInputManager if we are.
		if (!(config.getBoolean("noinput")))
			consoleInputManager(im);
	}
}
