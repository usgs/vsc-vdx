package gov.usgs.volcanoes.vdx.in.gps;

import gov.usgs.volcanoes.core.legacy.Arguments;
import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.time.J2kSec;
import gov.usgs.volcanoes.core.util.ResourceReader;
import gov.usgs.volcanoes.core.util.StringUtils;
import gov.usgs.volcanoes.vdx.data.Channel;
import gov.usgs.volcanoes.vdx.data.Column;
import gov.usgs.volcanoes.vdx.data.Rank;
import gov.usgs.volcanoes.vdx.data.SQLDataSource;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceHandler;
import gov.usgs.volcanoes.vdx.data.gps.SolutionPoint;
import gov.usgs.volcanoes.vdx.data.gps.GPS;
import gov.usgs.volcanoes.vdx.data.gps.SQLGPSDataSource;
import gov.usgs.volcanoes.vdx.in.Importer;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportTDP implements Importer {	

	private static final Logger LOGGER = LoggerFactory.getLogger(ImportTDP.class);

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
		LOGGER.info("ImportTDP.initialize() succeeded.");
		
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
		
		// define the data source handler that acts as a wrapper for data sources
		sqlDataSourceHandler	= new SQLDataSourceHandler(driver, url, prefix);
		
		// get the list of data sources that are being used in this import
		dataSource	= params.getString("dataSource");
				
		// lookup the data source from the list that is in vdxSources.config
		sqlDataSourceDescriptor	= sqlDataSourceHandler.getDataSourceDescriptor(dataSource);
		if (sqlDataSourceDescriptor == null) {
			LOGGER.error("{} sql data source does not exist in vdxSources.config", dataSource);
		}
				
		// formally get the data source from the list of descriptors.  this will initialize the data source which includes db creation
		sqlDataSource	= (SQLGPSDataSource)sqlDataSourceDescriptor.getSQLDataSource();
		
		if (!sqlDataSource.getType().equals(importerType)) {
			LOGGER.error("dataSource not a {} data source", importerType);
			System.exit(-1);
		}
		
		// information related to the timestamps
		dateIn	= new SimpleDateFormat("yyyy-M-d H:m:s");
		dateIn.setTimeZone(TimeZone.getTimeZone("GMT"));
		
		// get the list of ranks that are being used in this import
		rankParams		= params.getSubConfig("rank");
		rankName		= StringUtils.stringToString(rankParams.getString("name"), "Raw Data");
		rankValue		= StringUtils.stringToInt(rankParams.getString("value"), 1);
		rankDefault		= StringUtils.stringToInt(rankParams.getString("default"), 0);
		rank			= new Rank(0, rankName, rankValue, rankDefault);
		
		// create rank entry
		if (sqlDataSource.getRanksFlag()) {
			Rank tempRank	= sqlDataSource.defaultGetRank(rank);
			if (tempRank == null) {
				tempRank = sqlDataSource.defaultInsertRank(rank);
			}
			if (tempRank == null) {
				LOGGER.error("invalid rank for dataSource {}", dataSource);
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
	
	/*
	 * parse tdp file from url (resource locator or filename)
	 * @param filename
	 */
	public void process(String filename) {

		String			oneLine, sortValue, code, direction;
		String[]		oneLineArray;
		double			j2ksec, prevj2ksec, val, sval, llh[];
		int				sid, count, numberOfLines;		
		Date 			date;
		Channel 		channel;
		SolutionPoint 	sp;
		SolutionPoint[]	points;
		Observation[]	observations;

		try {
			
			// check that the file exists
			rr = ResourceReader.getResourceReader(filename);
			if (rr == null) {
				LOGGER.error("skipping: {} (resource is invalid)", filename);
				return;
			}

			// count the lines in the file
			numberOfLines	= 0;
			while (rr.nextLine() != null) {
				numberOfLines++;
			}
			rr.close();
			
			// if the file is empty then exit
			if (numberOfLines == 0) {
				LOGGER.error("skipping: {}(resource has 0 lines)", filename);
				return;
			}

			// print out a confirmation message that we are getting going
			LOGGER.info("filename: {}", filename);
			LOGGER.info("lines: {}", numberOfLines);

			// now read the file again and lets get all the information out of it
			rr 				= ResourceReader.getResourceReader(filename);
			observations	= new Observation[numberOfLines];
			points			= new SolutionPoint[numberOfLines / 3];

			// keep track of which data line we are on, for storage into the array
			count	= 0;

			// read the file line by line and store in an array
			while ((oneLine = rr.nextLine()) != null) {

				// read the line
				// oneLineArray	= oneLine.split("\\s+");
				oneLineArray	= oneLine.split(",");

				// get the j2ksec from the date
				date		= dateIn.parse(oneLineArray[0]);
				date.setTime(date.getTime());
				j2ksec		= J2kSec.fromDate(date);

				// get the values for this observation
				val			= Double.parseDouble(oneLineArray[2]) * 1000;
				sval		= Double.parseDouble(oneLineArray[3]);

				// get the direction for this observation
				direction		= oneLineArray[5].trim();

				// turn the station code into a station ID
				code			= oneLineArray[6].trim();
				channel			= channelMap.get(code);

				// define a sorting value
				sortValue = String.valueOf(j2ksec) + "  " + code + "  " + direction;

				// save this station into the station array
				observations[count] = new Observation(sortValue, j2ksec, code, direction, val, sval);

				// increment the count
				count++;

			}

			// sort the array 
			Arrays.sort(observations);

			// store the data in the database
			for (int i = 0; i < numberOfLines / 3; i++) {
				
				sp	= new SolutionPoint();
				
				sp.channel	= observations[i * 3 + 0].code;
				sp.dp.t		= observations[i * 3 + 0].j2ksec;
				sp.dp.x		= observations[i * 3 + 0].val;
				sp.dp.sxx	= observations[i * 3 + 0].sval;
				sp.dp.y		= observations[i * 3 + 1].val;
				sp.dp.syy	= observations[i * 3 + 1].sval;
				sp.dp.z		= observations[i * 3 + 2].val;
				sp.dp.szz	= observations[i * 3 + 2].sval;
				
				points[i]	= sp;
			}
			
			rr.close();
			
			prevj2ksec	= -1;
			sid			= -1;
			for (SolutionPoint spt : points) {
				
				channel	= channelMap.get(spt.channel);
				
				// if the channel isn't in the channel list from the db then it needs to be created
				if (channel == null) {
					llh	= GPS.xyz2LLH(spt.dp.x, spt.dp.y, spt.dp.z);
					sqlDataSource.createChannel(spt.channel, spt.channel, llh[0], llh[1], llh[2], 1);
					channel	= sqlDataSource.getChannel(spt.channel);
					channelMap.put(spt.channel, channel);
				}
				
				// get the j2ksec and create a new source (if necessary)
				j2ksec	= spt.dp.t;
				if (j2ksec != prevj2ksec) {
					sid = sqlDataSource.insertSourceSimple(new File(filename).getName(), j2ksec, j2ksec, rid);
					prevj2ksec = j2ksec;
				}
				
				// insert the solution into the db
				sqlDataSource.insertSolution(sid, channel.getCId(), spt.dp);
			}

		} catch (Exception e) {
			LOGGER.error("ImportTDP.process({}) failed.", filename, e);
		}
	}

	public void outputInstructions(String importerClass, String message) {
		if (message == null) {
			System.err.println(message);
		}
		System.err.println(importerClass + " -c configfile filelist");
	}

	@SuppressWarnings("rawtypes")
	public class Observation implements Comparable {

		public String	sortValue, code, direction;
		public double	j2ksec, val, sval;

		public Observation(String sortValue, double j2ksec, String code, String direction, double val, double sval) {
			this.sortValue	= sortValue;
			this.j2ksec		= j2ksec;
			this.code		= code;
			this.direction	= direction;
			this.val		= val;
			this.sval		= sval;
		}

		public int compareTo(Object obj) {
			Observation tmp = (Observation)obj;
			return this.sortValue.compareTo(tmp.sortValue);
		}
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
		
		ImportTDP importer	= new ImportTDP();
		
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
