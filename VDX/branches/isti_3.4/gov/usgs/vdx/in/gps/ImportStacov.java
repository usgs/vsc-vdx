package gov.usgs.vdx.in.gps;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.Channel;
import gov.usgs.vdx.data.Column;
import gov.usgs.vdx.data.Rank;
import gov.usgs.vdx.data.SQLDataSource;
import gov.usgs.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.vdx.data.SQLDataSourceHandler;
import gov.usgs.vdx.data.gps.SolutionPoint;
import gov.usgs.vdx.data.gps.GPS;
import gov.usgs.vdx.data.gps.SQLGPSDataSource;
import gov.usgs.vdx.in.Importer;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

/**
 * Import Stacov files
 * 
 * @author Dan Cervelli, Loren Antolik
 */
public class ImportStacov implements Importer {
	
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
	public SQLGPSDataSource sqlDataSource;
	public SQLDataSourceHandler sqlDataSourceHandler;
	public SQLDataSourceDescriptor sqlDataSourceDescriptor;	
	public List<String> dataSourceList;
	public Iterator<String> dsIterator;
	public Map<String, SQLDataSource> sqlDataSourceMap;
	public Map<String, String> dataSourceColumnMap;
	public Map<String, String> dataSourceChannelMap;
	public Map<String, Integer>	dataSourceRankMap;
	
	public Rank rank;
	public String rankName;
	public int rankValue, rankDefault;
	public int rid;
	
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
	
	public String importerType = "gps";
	
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
	 * @param importerClass name of importer class to use
	 * @param configFile configuration file
	 * @param verbose true for info, false for severe
	 */
	public void initialize(String importerClass, String configFile, boolean verbose) {
		
		// initialize the logger for this importer
		logger	= Logger.getLogger(importerClass);
		logger.log(Level.INFO, "ImportStacov.initialize() succeeded.");
		
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
			logger.log(Level.SEVERE, "%s was not successfully read", configFile);
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
		
		// define the data source handler that acts as a wrapper for data sources
		sqlDataSourceHandler	= new SQLDataSourceHandler(driver, url, prefix);
		
		// get the list of data sources that are being used in this import
		dataSource	= params.getString("dataSource");
				
		// lookup the data source from the list that is in vdxSources.config
		sqlDataSourceDescriptor	= sqlDataSourceHandler.getDataSourceDescriptor(dataSource);
		if (sqlDataSourceDescriptor == null) {
			logger.log(Level.SEVERE, dataSource + " sql data source does not exist in vdxSources.config");
		}
				
		// formally get the data source from the list of descriptors.  this will initialize the data source which includes db creation
		sqlDataSource	= (SQLGPSDataSource)sqlDataSourceDescriptor.getSQLDataSource();
		
		if (!sqlDataSource.getType().equals(importerType)) {
			logger.log(Level.SEVERE, "dataSource not a " + importerType + " data source");
			System.exit(-1);
		}
		
		// information related to the timestamps
		timestampMask	= "yyMMMdd";
		timeZone		= "GMT";
		dateIn			= new SimpleDateFormat(timestampMask);
		dateIn.setTimeZone(TimeZone.getTimeZone(timeZone));
		
		// get the list of ranks that are being used in this import
		rankParams		= params.getSubConfig("rank");
		rankName		= Util.stringToString(rankParams.getString("name"), "DEFAULT");
		rankValue		= Util.stringToInt(rankParams.getString("value"), 1);
		rankDefault		= Util.stringToInt(rankParams.getString("default"), 0);
		rank			= new Rank(0, rankName, rankValue, rankDefault);
		
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
			rid	= tempRank.getId();
		}
		
		// get the list of channels and create a hash map keyed with the channel code
		List<Channel> chs	= sqlDataSource.getChannelsList();
		channelMap			= new HashMap<String, Channel>();
		for (Channel ch : chs) {
			channelMap.put(ch.getCode(), ch);
		}
	}
	
	/**
	 * Parse stacov file from url (resource locator or filename)
	 * @param filename
	 */
	public void process(String filename) {
		
		// initialize variables local to this method
		String sx, sy, sz, sc;
		int p1, p2, i1, i2;
		double data;
		double llh[];
		SolutionPoint sp;
		Channel channel;
		boolean done;
		
		try {
			
			// check that the file exists
			rr = ResourceReader.getResourceReader(filename);
			if (rr == null) {
				logger.log(Level.SEVERE, "skipping: " + filename + " (resource is invalid)");
				return;
			}
			
			// move to the first line in the file
			String line		= rr.nextLine();
			
			// check that the file has data
			if (line == null) {
				logger.log(Level.SEVERE, "skipping: " + filename + " (resource is empty)");
				return;
			}
			
			// read the first line and get soltion count information
			int numParams			= Integer.parseInt(line.substring(0, 5).trim());
			SolutionPoint[] points	= new SolutionPoint[numParams / 3];

			// read the first line and get date information, add 12 hours to the date to get it in the middle of the day
			double j2ksec0, j2ksec1;
			try {
				String timestamp	= line.substring(20, 27);
				date				= dateIn.parse(timestamp);
				date.setTime(date.getTime() + 12 * 60 * 60 * 1000);
				j2ksec0				= Util.dateToJ2K(date);
				j2ksec1				= j2ksec0 + 86400;
			} catch (ParseException e) {
				logger.log(Level.SEVERE, "skipping: " + filename + "  (timestamp not valid)");
				return;
			}
			
			// attempt to insert this source.  this method will tell if this file has already been imported
			int sid	= sqlDataSource.insertSource(new File(filename).getName(), Util.md5Resource(filename), j2ksec0, j2ksec1, rid);
			if (sid == -1) {
				logger.log(Level.SEVERE, "skipping: " + filename + " (hash already exists)");
				return;
			}
			
			logger.log(Level.INFO, "importing: " + filename);
			
			for (int i = 0; i < numParams / 3; i++) {
				sx	= rr.nextLine();
				sy	= rr.nextLine();
				sz	= rr.nextLine();
				sp	= new SolutionPoint();
				
				sp.channel	= sx.substring(7, 11).trim();
				sp.dp.t		= (j2ksec0 + j2ksec1) / 2;
				sp.dp.x		= Double.parseDouble(sx.substring(25, 47).trim());
				sp.dp.sxx	= Double.parseDouble(sx.substring(53, 74).trim());
				
				sp.dp.y		= Double.parseDouble(sy.substring(25, 47).trim());
				sp.dp.syy	= Double.parseDouble(sy.substring(53, 74).trim());
				
				sp.dp.z		= Double.parseDouble(sz.substring(25, 47).trim());
				sp.dp.szz	= Double.parseDouble(sz.substring(53, 74).trim());
				
				points[i]	= sp;
			}
			
			done = false;
			while (!done) {
				try {
					sc = rr.nextLine();
					if (sc != null && sc.length() >= 2) {
						p1		= Integer.parseInt(sc.substring(0, 5).trim()) - 1;
						p2		= Integer.parseInt(sc.substring(5, 11).trim()) - 1;
						data	= Double.parseDouble(sc.substring(13).trim());
						if (p1 / 3 == p2 / 3) {
							sp	= points[p1 / 3];
							i1	= Math.min(p1 % 3, p2 % 3);
							i2	= Math.max(p1 % 3, p2 % 3);
							if (i1 == 0 && i2 == 1)
								sp.dp.sxy = data;
							else if (i1 == 0 && i2 == 2)
								sp.dp.sxz = data;
							else if (i1 == 1 && i2 == 2)
								sp.dp.syz = data;
						}
					} else {
						done = true;
					}
				} catch (NumberFormatException e) {
					done = true;	
				}
			}
			rr.close();
			for (SolutionPoint spt : points) {
				spt.dp.sxy = spt.dp.sxy * spt.dp.sxx * spt.dp.syy;
				spt.dp.sxz = spt.dp.sxz * spt.dp.sxx * spt.dp.szz;
				spt.dp.syz = spt.dp.syz * spt.dp.syy * spt.dp.szz;
				spt.dp.sxx = spt.dp.sxx * spt.dp.sxx;
				spt.dp.syy = spt.dp.syy * spt.dp.syy;
				spt.dp.szz = spt.dp.szz * spt.dp.szz;
				
				channel	= channelMap.get(spt.channel);
				
				// if the channel isn't in the channel list from the db then it needs to be created
				if (channel == null) {
					llh	= GPS.xyz2LLH(spt.dp.x, spt.dp.y, spt.dp.z);
					sqlDataSource.createChannel(spt.channel, spt.channel, llh[0], llh[1], llh[2]);
					channel	= sqlDataSource.getChannel(spt.channel);
					channelMap.put(spt.channel, channel);
				}
				
				// insert the solution into the db
				sqlDataSource.insertSolution(sid, channel.getCID(), spt.dp);
			}

		} catch (Exception e) {
			logger.log(Level.SEVERE, "ImportStacov.process(" + filename + ") failed.", e);	
		}
	}
	
	public void outputInstructions(String importerClass, String message) {
		if (message == null) {
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
	 * @param as command line args
	 */
	public static void main(String as[]) {
		
		ImportStacov importer	= new ImportStacov();
		
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
