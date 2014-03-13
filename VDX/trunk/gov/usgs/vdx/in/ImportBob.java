package gov.usgs.vdx.in;

import gov.usgs.plot.data.GenericDataMatrix;
import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Time;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.Rank;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.SQLNullDataSource;
import gov.usgs.vdx.data.rsam.SQLEWRSAMDataSource;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

/**
 * import file with 2d data matrix into database
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.6  2007/06/06 20:22:55  tparker
 * EWRSAM rewrite
 *
 * Revision 1.5  2007/04/30 05:27:32  tparker
 * initial commit for rsam SQL Bob importer
 *
 * Revision 1.4  2007/04/23 22:37:19  dcervelli
 * Flipped value boolean test.
 *
 * Revision 1.3  2007/04/23 22:36:22  dcervelli
 * Only keeps good samples.
 *
 * Revision 1.2  2007/04/23 22:14:58  dcervelli
 * Fixed dt computation.
 *
 * Revision 1.1  2007/04/23 21:54:04  dcervelli
 * Initial commit.
 * 
 * @author Dan Cervelli
 * @version $Id: ImportBob.java,v 1.7 2007-06-12 20:44:29 tparker Exp $
 */
public class ImportBob implements Importer {
	
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
	public String timestamp;
	public String timezone;
	
	public Rank rank;
	public String rankName;
	public int rankValue, rankDefault;
	public int rid;
	
	public SQLDataSource sqlDataSource;
	
	public Logger logger;
	
	static {
		flags	= new HashSet<String>();
		keys	= new HashSet<String>();
		keys.add("-c");
		flags.add("-h");
		flags.add("-v");
	}
	
	
	
	
	
	
	
	
	
	public int goodCount;
	public double[] t;
	public float[] d;
	private int year;
	private static final String CONFIG_FILE = "VDX.config";
	//public ConfigFile params;
	Map<String, SQLDataSource> sources;

	/**
	 * Constructor
	 * 
	 * @param cf
	 *            configuration file name
	 * @param y
	 *            year
	 * @param n
	 *            vdx name
	 * @param t
	 *            type (events/values)
	 */
	public ImportBob(String cf, int y, String n, String t) {
		year = y;
		params = new ConfigFile(cf);
		params.put("vdx.name", n);
		if (params == null)
			System.out.println("Can't parse config file " + cf);
		params.put("type", t);

		sources = new HashMap<String, SQLDataSource>();
		sources.put("ewrsamEvents", new SQLEWRSAMDataSource("Events"));
		sources.put("ewrsamValues", new SQLEWRSAMDataSource("Values"));
		sources.put("null", new SQLNullDataSource());

	}

	/**
	 * takes a config file as a parameter and parses it to prepare for importing
	 * @param cf configuration file
	 * @param verbose true for info, false for severe
	 */
	public void initialize(String importerClass, String configFile, boolean verbose) {
		
		// initialize the logger for this importer
		logger	= Logger.getLogger(importerClass);
		logger.log(Level.INFO, "ImportBob.initialize() succeeded.");
		
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
		
		// ImportFile specific directives
		filenameMask	= Util.stringToString(params.getString("filenameMask"), "");
		headerLines		= Util.stringToInt(params.getString("headerLines"), 0);
		
		// information related to the timestamps
		timestamp		= Util.stringToString(params.getString("timestamp"), "yyyy-MM-dd HH:mm:ss");
		timezone		= Util.stringToString(params.getString("timezone"), "GMT");
		dateIn			= new SimpleDateFormat(timestamp);
		dateOut			= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateIn.setTimeZone(TimeZone.getTimeZone(timezone));
		dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));
		
		// get the list of ranks that are being used in this import
		rankParams		= params.getSubConfig("rank");
		rankName		= Util.stringToString(rankParams.getString("name"), "DEFAULT");
		rankValue		= Util.stringToInt(rankParams.getString("value"), 1);
		rankDefault		= Util.stringToInt(rankParams.getString("default"), 0);
		rank			= new Rank(0, rankName, rankValue, rankDefault);
		
	}
	
	/**
	 * Parse file from url (resource locator or file name)
	 * @param filename
	 */
	public void process(String filename) {
		
	}
	
	public void outputInstructions(String importerClass, String message) {
		if (message == null) {
			System.err.println(message);
		}
		System.err.println(importerClass + " -c configfile filelist");
	}

	/**
	 * @param c
	 *            channel code
	 * @param f
	 *            string contains 2-d matrix data
	 */
	public void process(String c, String f) {
		String type = params.getString("type");
		String name = params.getString("name");
		SQLDataSource sds = sources.get(type);

		if (sds == null) {
			System.out.println("I don't know what to do with type " + type);
			System.exit(-1);
		}

		// TODO: work out initialization
		// sds.defaultInitialize(params, name);
		
		// rework insert data functionality
		// sds.defaultInsertData(c, parseFile(f), sds.getTranslationsFlag(), sds.getRanksFlag(), 1);

	}

	/**
	 * @param fn
	 *            string contains 2-d matrix data
	 * @return DoubleMatrix2D parsed from file
	 */
	public DoubleMatrix2D parseFile(String fn) {
		DoubleMatrix2D data = null;

		try {
			DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(fn)));
			int absoluteRecordSize = Util.swap(dis.readShort());
			int samplesPerRecord = absoluteRecordSize / 4;

			double dt = 86400.0 / samplesPerRecord;
			double time = Time.parse("yyyyMMDD", year + "0101");
			dis.readShort(); // skip remaining 16-bits
			dis.skip(absoluteRecordSize - 4);
			GregorianCalendar cal = new GregorianCalendar();
			boolean leapYear = cal.isLeapYear(year);
			int numRecords = 365;
			if (leapYear)
				numRecords++;

			data = DoubleFactory2D.dense.make(numRecords * samplesPerRecord, 2);
			System.err.println("records: " + numRecords);
			System.err.println("record size: " + absoluteRecordSize);
			System.err.println("expected samples: " + numRecords * samplesPerRecord);
			System.err.println("expected filesize: " + absoluteRecordSize * (numRecords + 1));
			goodCount = 0;
			for (int i = 0; i < numRecords * samplesPerRecord; i++) {
				float value = Float.intBitsToFloat(Util.swap(dis.readInt()));
				if (value != -998.0f) {
					data.setQuick(goodCount, 0, time);
					data.setQuick(goodCount++, 1, value);
					// System.out.println(t[i] + "," + d[i]);
				}
				time += dt;
			}
			System.err.println("good count: " + goodCount);
			dis.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return (data);
	}

	/**
	 * Main method.
	 * Possible command line arguments:
	 * 
	 * -c configuration file name
	 * -h prints help message
	 * -s station
	 * -y year
	 * -n name
	 * -t type
	 * @param as command line args
	 */
	public static void main(String[] as) {
		
		ImportFile importer	= new ImportFile();
		
		Arguments args = new Arguments(as, flags, keys);
		
		if (args.flagged("-h")) {
			importer.outputInstructions(importer.getClass().getName(), null);
			System.exit(-1);
		}
		
		if (args.contains("-c")) {
			importer.initialize(importer.getClass().getName(), args.get("-c"), args.flagged("-v"));
			importer.outputInstructions(importer.getClass().getName(), "Config file required");
			System.exit(-1);
		}

		importer.initialize(importer.getClass().getName(), args.get("-c"), args.flagged("-v"));

		List<String> files	= args.unused();
		for (String file : files) {
			importer.process(file);
		}
		
		importer.deinitialize();
		
		
		
		
		
		
		/*

		String cf = CONFIG_FILE;
		String name = "";
		String type = "";
		int year = 1970;
		String channel = "";

		Set<String> flags = new HashSet<String>();
		Set<String> kvs = new HashSet<String>();
		kvs.add("-c");
		kvs.add("-h");
		kvs.add("-s");
		kvs.add("-y");
		kvs.add("-n");
		kvs.add("-t");

		Arguments args = new Arguments(as, flags, kvs);

		if (args.contains("-c"))
			cf = args.get("-c");

		if (args.contains("-n"))
			name = args.get("-n");

		if (args.contains("-s"))
			channel = args.get("-s");

		if (args.contains("-y"))
			year = Integer.parseInt(args.get("-y"));

		if (args.contains("-t"))
			type = args.get("-t");

		if (args.contains("-h") || type == null || type == "") {
			System.err.println("java gov.usgs.vdx.data.gps.ImportBob [-c configFile] -n <name> -s <station> -y <year> -t <type>");
			System.err.println("Known types: \n\tewrsamEvents \n\tewrsamValues \n\tnull");
			System.exit(-1);
		}

		List<String> files = args.unused();
		ImportBob in = new ImportBob(cf, year, name, type);

		for (String f: files)
			in.process(channel, f);
		 */
	}
}