package gov.usgs.vdx.in.hypo;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.Rank;
import gov.usgs.vdx.data.SQLDataSourceHandler;
import gov.usgs.vdx.data.hypo.Hypocenter;
import gov.usgs.vdx.data.hypo.SQLHypocenterDataSource;
import gov.usgs.vdx.in.Importer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import java.util.logging.Level;

/**
 * Import HypoInverse files
 *  
 * @author Loren Antolik
 */
public class ImportHypoInverse extends Importer {
	
	private String importerType	= "hypocenters";
	
	private SQLHypocenterDataSource sqlDataSource;

	/**
	 * takes a config file as a parameter and parses it to prepare for importing
	 * @param cf configuration file
	 * @param verbose true for info, false for severe
	 */
	public void initialize(String importerClass, String configFile, boolean verbose) {
		defaultInitialize(importerClass, verbose);
		
		// process the config file
		processConfigFile(configFile);
	}
	
	/**
	 * Parse configuration file.  This sets class variables used in the importing process
	 * @param configFile	name of the config file
	 */
	public void processConfigFile(String configFile) {
		
		logger.log(Level.INFO, "Reading config file " + configFile);
		
		// instantiate the config file
		params		= new ConfigFile(configFile);
		
		// get the vdx parameter, and exit if it's missing
		vdxConfig	= params.getString("vdx.config");
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
		sqlDataSource	= (SQLHypocenterDataSource)sqlDataSourceDescriptor.getSQLDataSource();
		
		if (!sqlDataSource.getType().equals(importerType)) {
			logger.log(Level.SEVERE, "dataSource not a " + importerType + " data source");
			System.exit(-1);
		}
		
		// information related to the timestamps
		timestampMask	= "yyyyMMddHHmmssSS";
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
			rank = sqlDataSource.defaultInsertRank(rank);
			if (rank == null) {
				logger.log(Level.SEVERE, "invalid rank");
				System.exit(-1);
			}
		}
	}
	
	/**
	 * Parse hypoinverse file from url (resource locator or file name)
	 * @param filename
	 */
	public void process(String filename) {
		
		// initialize variables local to this method
		double j2ksec, lat, lon, depth, prefmag;
		String eid;
		Double ampmag	= Double.NaN;
		Double codamag	= Double.NaN;
		Double dmin		= Double.NaN;
		Double rms		= Double.NaN;
		Double herr		= Double.NaN;
		Double verr		= Double.NaN;
		String magtype	= null;
		String rmk		= null;
		Integer nphases	= null;
		Integer azgap	= null;
		Integer nstimes	= null;
		
		try {
			
			// check that the file exists
			rr = ResourceReader.getResourceReader(filename);
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
			
			logger.log(Level.INFO, "importing: " + filename);

			while (line != null) {
				
				// do some validation
				if (!line.substring(45,46).equals(" ")) {
					logger.log(Level.SEVERE, "skipping: line number " + lineNumber + ".  Corrupt data at column 46.");					
					line	= rr.nextLine();
					lineNumber++;
					continue;
					
				// all systems go then
				} else {					
					logger.log(Level.INFO, "importing: line number " + lineNumber);
				}
				
				// DATE
				try {
					String timestamp	= line.substring(0,16) + "0";
					date				= dateIn.parse(timestamp);
					j2ksec				= Util.dateToJ2K(date);
				} catch (ParseException e) {
					logger.log(Level.SEVERE, "skipping: line number " + lineNumber + ".  Timestamp not valid.");					
					line	= rr.nextLine();
					lineNumber++;
					continue;
				}
				
				// EID
				eid			= line.substring(136, 146).trim();
				if (eid.trim().length() == 0) {
					logger.log(Level.SEVERE, "skipping: line number " + lineNumber + ".  Event ID not valid.");					
					line	= rr.nextLine();
					lineNumber++;
					continue;
				}

				// LAT
				double latdeg	= Double.parseDouble(line.substring(16, 18).trim());
				double latmin	= Double.parseDouble(line.substring(19, 21).trim() + "." + line.substring(21, 23).trim());
				lat				= latdeg + ( latmin / 60.0d );
				char ns			= line.charAt(18);
				if (ns == 'S')
					lat *= -1;

				// LON
				double londeg	= Double.parseDouble(line.substring(23, 26).trim());
				double lonmin	= Double.parseDouble(line.substring(27, 29).trim() + "." + line.substring(29, 31).trim());
				lon				= londeg + ( lonmin / 60.0d );
				char ew			= line.charAt(26);
				if (ew != 'E')
					lon *= -1;
				
				// DEPTH
				try {
					depth		= Double.parseDouble(line.substring(31, 34).trim() + "." + line.substring(34, 36).trim());
					depth *= -1;
				} catch (NumberFormatException e) {
					logger.log(Level.SEVERE, "skipping: line number " + lineNumber + ".  Depth not valid.");					
					line	= rr.nextLine();
					lineNumber++;
					continue;
				}
				
				// PREFERRED MAGNITUDE
				try {
					prefmag 	= Double.parseDouble(line.substring(147, 150).trim()) / 100;
				} catch (NumberFormatException e) {
					prefmag		= Double.NaN;
				}				
				
				// AMPLITUDE MAGNITUDE
				try {				
					ampmag 		= Double.parseDouble(line.substring(36, 39).trim()) / 100;
				} catch (NumberFormatException e) {
					ampmag		= Double.NaN;
				}
				
				// CODA MAGNITUDE
				try {
					codamag 	= Double.parseDouble(line.substring(70, 73).trim()) / 100;
				} catch (NumberFormatException e) {
					codamag		= Double.NaN;
				}
				
				// NPHASES
				try {
					nphases		= Integer.parseInt(line.substring(39, 42).trim());
				} catch (NumberFormatException e) {
					nphases		= 0;
				}
				
				// AZGAP
				try {
					azgap		= Integer.parseInt(line.substring(42, 45).trim());
				} catch (NumberFormatException e) {
					azgap		= null;
				}
				
				// DMIN
				try {
					dmin		= Double.parseDouble(line.substring(45, 48).trim());
				} catch (NumberFormatException e) {
					dmin		= Double.NaN;
				}
				
				// RMS
				try {
					rms			= Double.parseDouble(line.substring(48, 52).trim()) / 100;
				} catch (NumberFormatException e) {
					rms			= Double.NaN;
				}
				
				// NSTIMES
				try {
					nstimes		= Integer.parseInt(line.substring(82, 85).trim());
				} catch (NumberFormatException e) {
					nstimes		= 0;
				}
				
				// HERR
				try {
					herr		= Double.parseDouble(line.substring(85, 89).trim()) / 100;
				} catch (NumberFormatException e) {
					herr		= Double.NaN;
				}
				
				// VERR
				try {
					verr		= Double.parseDouble(line.substring(89, 93).trim()) / 100;
				} catch (NumberFormatException e) {
					verr		= Double.NaN;
				}
				
				// RMK
				rmk			= line.substring(80, 81).trim();
				if (rmk.trim().length() == 0) {
					rmk		= null;
				}
				
				// MAGTYPE
				magtype		= line.substring(146, 147).trim();
				if (magtype.trim().length() == 0) {
					magtype		= null;
				}
				
				Hypocenter hc	= new Hypocenter(j2ksec, eid, rank.getId(), lat, lon, depth, prefmag, ampmag, codamag, 
						nphases, azgap, dmin, rms, nstimes, herr, verr, magtype, rmk);
				sqlDataSource.insertHypocenter(hc);
				
				line	= rr.nextLine();
				lineNumber++;
			}
				
		} catch (Exception e) {
			logger.log(Level.SEVERE, "ImportHypoInverse.process(" + filename + ") failed.", e);
		}
	}
	
	public void outputInstructions(String importerClass, String message) {
		defaultOutputInstructions(importerClass, message);
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
		
		ImportHypoInverse importer	= new ImportHypoInverse();
		
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
	}	
}
