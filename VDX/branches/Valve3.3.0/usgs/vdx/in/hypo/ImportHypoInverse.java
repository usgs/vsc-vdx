package gov.usgs.vdx.in.hypo;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.Rank;
import gov.usgs.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.vdx.data.SQLDataSourceHandler;
import gov.usgs.vdx.data.hypo.Hypocenter;
import gov.usgs.vdx.data.hypo.SQLHypocenterDataSource;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * Class for importing hypo71 format catalog files.
 *  
 * @author Loren Antolik
 */
public class ImportHypoInverse {
	
	private Logger logger;
	
	private ConfigFile params;
	private ConfigFile vdxParams;
	private ConfigFile rankParams;
	
	private String driver, prefix, url;

	private int headerLines;
	private String timestampMask;
	private String timezone;
	
	private SimpleDateFormat dateIn;
	private SimpleDateFormat dateOut;
	
	private Rank rank;
	private String rankCode, rankName;
	private int rankValue, rankDefault;
	private List<String> rankList;
	private Map<String, Rank> rankMap;
	
	private String dataSource;
	private SQLHypocenterDataSource sqlHypocenterDataSource;
	private SQLDataSourceHandler sqlDataSourceHandler;
	private SQLDataSourceDescriptor sqlDataSourceDescriptor;
	
	int count;
	int test;

	/**
	 * takes a config file as a parameter and parses it to prepare for importing
	 * @param cf configuration file
	 */
	public void initialize(String configFile, boolean verbose) {
		
		// initialize the logger		
		logger = Logger.getLogger("gov.usgs.vdx.in.hypo.ImportHypoInverse");		
		if (verbose) {
			logger.setLevel(Level.ALL);
		} else {
			logger.setLevel(Level.INFO);
		}
		
		// process the config file
		processConfigFile(configFile);
	}
	
	/**
	 * Parse configuration file.  This sets class variables used in the importing process
	 * @param configFile	name of the config file
	 */
	private void processConfigFile(String configFile) {
		
		logger.log(Level.INFO, "Reading config file " + configFile);
		
		// instantiate the config file
		params		= new ConfigFile(configFile);
		
		// get the vdx parameter, and exit if it's missing
		String vdxConfig	= params.getString("vdx.config");
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
		
		// information related to header lines in this file
		headerLines	= Util.stringToInt(params.getString("headerlines"), 0);
		
		// information related to the timestamps
		dateIn			= new SimpleDateFormat(timestampMask);
		dateOut			= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		dateIn.setTimeZone(TimeZone.getTimeZone(timezone));
		dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));
		
		// get the list of ranks that are being used in this import
		rankList					= params.getList("rank");
		if (rankList != null) {
			Iterator<String> rkIterator	= rankList.iterator();
			rankMap						= new HashMap<String, Rank>();
			while (rkIterator.hasNext()) {
				rankCode		= params.getString("rank");
				rankParams		= params.getSubConfig(rankCode);
				rankName		= Util.stringToString(rankParams.getString("name"), rankCode);
				rankValue		= Util.stringToInt(rankParams.getString("value"), 1);
				rankDefault		= Util.stringToInt(rankParams.getString("default"), 1);
				rank			= new Rank(0, rankName, rankValue, rankDefault);
				rankMap.put(rankCode, rank);
			}
		}
		
		// get the list of data sources that are being used in this import
		dataSource	= params.getString("dataSource");
				
		// lookup the data source from the list that is in vdxSources.config
		sqlDataSourceDescriptor	= sqlDataSourceHandler.getDataSourceDescriptor(dataSource);
		if (sqlDataSourceDescriptor == null) {
			logger.log(Level.SEVERE, dataSource + " sql data source does not exist in vdxSources.config");
		}
				
		// formally get the data source from the list of descriptors.  this will initialize the data source which includes db creation
		sqlHypocenterDataSource	= (SQLHypocenterDataSource) sqlDataSourceDescriptor.getSQLDataSource();
		
		if (sqlHypocenterDataSource.getType().equals("hypocenter")) {
			logger.log(Level.SEVERE, "dataSource not a hypocenters data source");
			System.exit(-1);
		}
				
		// create rank entry
		if (sqlHypocenterDataSource.getRanksFlag()) {
			sqlHypocenterDataSource.defaultInsertRank(rank);
		}
	}
	
	/**
	 * Parse H71 file from url (resource locator or file name)
	 */
	public void process(String filename) {
		
		try {
			
			// check that the file exists
			ResourceReader rr = ResourceReader.getResourceReader(filename);
			if (rr == null) {
				logger.log(Level.SEVERE, "skipping: " + filename + " (resource is invalid)");
				return;
			}
			
			// move to the first line in the file
			String line		= rr.nextLine();
			int lineNumber	= 1;
			
			// check that the file has data
			if (line == null) {
				logger.log(Level.SEVERE, "skipping: " + filename + " (resource is empty)");
				return;
			}
			
			logger.info("importing: " + filename);
			
			// if any header lines are defined then skip them
			if (headerLines > 0) {
				logger.log(Level.INFO, "skipping " + headerLines + " header lines");
				for (int i = 0; i < headerLines; i++) {
					line	= rr.nextLine();
					lineNumber++;
				}
			}
			
			// we are now at the first row of data.  time to import!
			while (line != null) {
				
				// DATE
				String year		= line.substring(0,4);
				String monthDay	= line.substring(4,8);
				String hourMin	= line.substring(8,12);
				String sec		= line.substring(12,14);
				String secDec	= line.substring(14,16);
				
				Date date		= dateIn.parse(year + monthDay + hourMin + sec + "." + secDec);
				double j2ksec	= Util.dateToJ2K(date);

				// LAT
				double latdeg	= Double.parseDouble(line.substring(16, 18).trim());
				double latmin	= Double.parseDouble(line.substring(19, 21).trim() + "." + line.substring(21, 23).trim());
				double lat		= latdeg + ( latmin / 60.0d );
				char ns			= line.charAt(18);
				if (ns == 'S')
					lat *= -1;


				// LON
				double londeg	= Double.parseDouble(line.substring(23, 26).trim());
				double lonmin	= Double.parseDouble(line.substring(27, 29).trim() + "." + line.substring(29, 31).trim());
				double lon		= londeg + ( lonmin / 60.0d );
				char ew			= line.charAt(26);
				if (ew != 'E')
					lon *= -1;
				
				// DEPTH
				double depth	= Double.parseDouble(line.substring(31, 34).trim() + "." + line.substring(34, 36).trim());
				depth *= -1;
				
				// MAGNITUDE
				double mag		= -99.99;
				try { mag = Double.parseDouble(line.substring(147, 150).trim()) / 100; } catch (Exception pe) {}
				
				if (!line.substring(45,46).equals(" "))
					throw new Exception("corrupt data at column 46");
				
				Hypocenter hc	= new Hypocenter(new double[] {j2ksec, lon, lat, depth, mag});
				sqlHypocenterDataSource.insertHypocenter(hc);
			}
				
		} catch (Exception e) {
			logger.log(Level.SEVERE, "ImportHypoInverse.process(" + filename + ") failed.", e);
		}
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
		
		String configFile;
		boolean verbose;
		Set<String> flags;
		Set<String> keys;
		
		flags	= new HashSet<String>();
		keys	= new HashSet<String>();
		keys.add("-c");
		flags.add("-h");
		flags.add("--help");
		flags.add("-v");
		
		Arguments args = new Arguments(as, flags, keys);
		
		if (args.flagged("-h") || args.flagged("--help")) {
			System.err.println("java gov.usgs.vdx.in.ImportFile [-c configFile]");
			System.exit(-1);
		}
		
		if (!args.contains("-c")) {
			System.err.println("config file required");
			System.exit(-1);
		}
		
		ImportHypoInverse ihi	= new ImportHypoInverse();

		configFile	= args.get("-c");
		verbose		= args.flagged("-v");
		ihi.initialize(configFile, verbose);

		List<String> files	= args.unused();
		for (String file : files) {
			ihi.process(file);
		}
	}	
}
