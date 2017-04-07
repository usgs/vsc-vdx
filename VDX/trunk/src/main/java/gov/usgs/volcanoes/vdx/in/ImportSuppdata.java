package gov.usgs.volcanoes.vdx.in;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.ResourceReader;
import gov.usgs.volcanoes.vdx.data.Channel;
import gov.usgs.volcanoes.vdx.data.Column;
import gov.usgs.volcanoes.vdx.data.DataSourceDescriptor;
import gov.usgs.volcanoes.vdx.data.DataSourceHandler;
import gov.usgs.volcanoes.vdx.data.SuppDatum;
import gov.usgs.volcanoes.vdx.data.SQLDataSource;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceHandler;
import gov.usgs.volcanoes.vdx.data.VDXSource;
import gov.usgs.volcanoes.vdx.db.VDXDatabase;

public class ImportSuppdata {
	private static Set<String> flags;
	private static Set<String> keys;
	private Logger logger;
	private Map<String, Integer> channelMap;	
	private Map<String, Integer> columnMap;	
	private Map<String, Integer> rankMap;	
	private Map<String, Integer> sdtypeMap;	
	private VDXDatabase database;
	private ConfigFile vdxParams;
	private DataSourceHandler dataSourceHandler;
	private SQLDataSourceHandler sqlDataSourceHandler;

	static {
		flags	= new HashSet<String>();
		keys	= new HashSet<String>();
		keys.add("-c");
		flags.add("-h");
		flags.add("-cm");
		flags.add("-v");
	}

	/**
	 * Initialize importer.
	 */
	public void initialize(String importerClass, String configFile) {
		
		// initialize the logger for this importer
		logger	= Logger.getLogger(importerClass);
		logger.info( "ImportSuppdata.initialize() succeeded.");
		
		// process the config file
		processConfigFile(configFile);
	}
	
	/**
	 * Deinitialize importer.
	 */
	public void deinitialize() {
		database.close();
	}
	
	/**
	 * Process config file.  Reads a config file and parses contents into local variables
	 */
	public void processConfigFile(String vdxConfig) {
		
		// get the vdx config as it's own config file object
		vdxParams 			 = new ConfigFile(vdxConfig);
		String driver		 = vdxParams.getString("vdx.driver");
		String url			 = vdxParams.getString("vdx.url");
		String prefix		 = vdxParams.getString("vdx.prefix");
		database			 = new VDXDatabase(driver, url, prefix);
		dataSourceHandler 	 = new DataSourceHandler(driver, url, prefix);
		sqlDataSourceHandler = new SQLDataSourceHandler(driver, url, prefix);
	}
	
	/**
	 * Process.  Reads a file and parses the contents to the database
	 */
	public void process(String pathname) {
		File file = new File( pathname );
		String filename = file.getName();
		String source = filename.substring( 5, filename.length()-19 );
		SQLDataSourceDescriptor dsd = sqlDataSourceHandler.getDataSourceDescriptor(source);
		if (dsd == null) {
			logger.log(Level.SEVERE, "skipping: " + pathname + " (datasource is invalid)");
			return;
		}
		SQLDataSource ds = null;
		VDXSource vds = null;
		try {
			Object ods = dsd.getSQLDataSource();
			if (ods != null) {
				ds = dsd.getSQLDataSource();
			}
		} catch (Exception e) {
		}
		
		int defaultRank = 0;
		int lineLen1, lineLen2;
		if ( ds != null ) {
			// This is an SQL DataSource
			
			// Build map of channels
			channelMap = new HashMap<String, Integer>();
			for ( Channel ch: ds.defaultGetChannelsList(false) )
				channelMap.put( ch.getCode(), ch.getCID() );
			logger.info( "Channels mapped: " + channelMap.size() );
			
			// Build map of columns
			columnMap = new HashMap<String, Integer>();
			for ( Column col: ds.defaultGetColumns(true,ds.getMenuColumnsFlag()) )
				columnMap.put( col.name, col.idx );
			logger.info( "Columns mapped: " + columnMap.size() );
			
			// Build map of ranks
			rankMap = new HashMap<String, Integer>();
			for ( String rk: ds.defaultGetRanks() ) {
				String rkBits[] = rk.split(":");
				int id = Integer.parseInt(rkBits[0]);
				rankMap.put( rkBits[1], id );
				if ( rkBits[3].equals("1") )
					defaultRank = id;
			}
			logger.info( "Ranks mapped: " + rankMap.size() );
			
			// Set limits on # args per input line
			lineLen1 = 7;
			lineLen2 = 9;
			
			// Build map of supp data types
			sdtypeMap = new HashMap<String, Integer>();
			for ( SuppDatum sdt: ds.getSuppDataTypes() )
				sdtypeMap.put( sdt.typeName, sdt.tid );
			logger.info( "Suppdata types mapped: " + sdtypeMap.size() );
			
		} else {
			// It isn't a SQL datasource; try it as a Winston datasource
			DataSourceDescriptor vdsd = dataSourceHandler.getDataSourceDescriptor(source);
			try {
				vds = (VDXSource)vdsd.getDataSource();
				if ( vds == null ) {
					logger.log(Level.SEVERE, "skipping: " + pathname + " (datasource is invalid)");
					return;
				}
			} catch (Exception e2) {
				logger.log(Level.SEVERE, "skipping: " + pathname + " (datasource is invalid)");
				return;
			}

			// Build map of channels
			channelMap = new HashMap<String, Integer>();
			for ( gov.usgs.winston.Channel ch: vds.getChannels().getChannels() )
				channelMap.put( ch.getCode(), ch.getSID() );
			logger.info( "Channels mapped: " + channelMap.size() );
			
			// Set limits on # args per input line
			lineLen1 = lineLen2 = 7;

			// Build map of supp data types
			sdtypeMap = new HashMap<String, Integer>();
			try {
				for ( SuppDatum sdt: vds.getSuppDataTypes() )
					sdtypeMap.put( sdt.typeName, sdt.tid );
				logger.info( "Suppdata types mapped: " + sdtypeMap.size() );
			} catch (Exception e3) {
				logger.log(Level.SEVERE, "skipping: " + pathname + " (problem reading supplemental data types)");
				return;
			}
		}
		
		// Access the input file
		ResourceReader rr = ResourceReader.getResourceReader(pathname);
		if (rr == null) {
			logger.log(Level.SEVERE, "skipping: " + pathname + " (resource is invalid)");
			return;
		}
		// move to the first line in the file
		String line		= rr.nextLine();
		int lineNumber	= 0;

		// check that the file has data
		if (line == null) {
			logger.log(Level.SEVERE, "skipping: " + pathname + " (resource is empty)");
			return;
		}
		
		logger.info("importing: " + filename);
		
		SuppDatum sd = new SuppDatum();
		int success = 0;
		
		// we are now at the first row of data.  time to import!
		String[] valueArray = new String[lineLen2];
		while (line != null) {
			lineNumber++;
			// Build up array of values in this line
			// First, we split it by quotes
			String[] quoteParts = line.split("'", -1);
			if ( quoteParts.length % 2 != 1 ) {
				logger.warning("Aborting import of line " + lineNumber + ", mismatched quotes");
				continue;
			}
			// Next, walk through those parts, splitting those outside of matching quotes by comma
			int valueArrayLength = 0;
			boolean ok = true;
			for ( int j=0; ok && j<quoteParts.length; j+=2 ) {
				String[] parts = quoteParts[j].split(",", -1);
				int k, k1 = 1, k2 = parts.length-1;
				boolean middle = true;
				if ( j==0 ) { // section before first quote
					middle = false;
					if ( parts.length > 1 && parts[0].trim().length() == 0 ) {
						logger.warning("Aborting import of line " + lineNumber + ", leading comma");
						ok = false;
						break;
					}
					k1 --;
				} 
				if ( j==quoteParts.length-1 ) { // section after last quote
					middle = false;
					if ( parts.length > 1 && parts[parts.length-1].trim().length() == 0 ) {
						logger.warning("Aborting import of line " + lineNumber + ", trailing comma");
						ok = false;
						break;
					}
					k2++;
				}
				if ( middle ) {
					if ( parts.length == 1 ){
						logger.warning("Aborting import of line " + lineNumber + ", missing comma between quotes");
						ok = false;
						break;
					}
					if ( parts[0].trim().length()!=0  ) {
						logger.warning("Aborting import of line " + lineNumber + ", missing comma after a quote");
						ok = false;
						break;
					}
					if ( parts[parts.length-1].trim().length()!=0 ) {
						logger.warning("Aborting import of line " + lineNumber + ", missing comma before a quote");
						ok = false;
						break;
					}
				}
				for ( k=k1; ok && k<k2; k++ ) {
					if ( valueArrayLength == lineLen2 ) {
						logger.warning("Aborting import of line " + lineNumber + ", too many elements");
						ok = false;
						break;
					}
					valueArray[valueArrayLength++] = parts[k];
				}
				if ( j+1 < quoteParts.length ) {
					if ( valueArrayLength == lineLen2 ) {
						logger.warning("Aborting import of line " + lineNumber + ", too many elements");
						ok = false;
						break;
					}
					valueArray[valueArrayLength++] = quoteParts[j+1];
				}
			}

			// Line has been parsed; get next one
			line	= rr.nextLine();
			if ( !ok )
				continue;
			
			// Validate & unmap arguments
			if ( valueArrayLength < lineLen1 ) {
				logger.warning("Aborting import of line " + lineNumber + ", too few elements (" + valueArrayLength + ")");
				continue;
			}
			try {
				sd.cid = channelMap.get( valueArray[3].trim() );
			} catch (Exception e) {
				logger.warning("Aborting import of line " + lineNumber + 
					", unknown channel: '" + valueArray[3] + "'");
				continue;
			}
			try {
				sd.st = Double.parseDouble( valueArray[1].trim() );
			} catch (Exception e) {
				logger.warning("Aborting import of line " + lineNumber + 
					", invalid start time: '" + valueArray[1] + "'");
				continue;
			}
			try {
				String et = valueArray[2].trim();
				if ( et.length() == 0 )
					sd.et = Double.MAX_VALUE;
				else
					sd.et = Double.parseDouble( et );
			} catch (Exception e) {
				logger.warning("Aborting import of line " + lineNumber + 
					", invalid end time: '" + valueArray[2] + "'");
				continue;
			}
			try {
				sd.typeName = valueArray[4].trim();
				Integer tid = sdtypeMap.get( sd.typeName );
				if ( tid == null ) {
					sd.color = "000000";
					sd.dl = 1;
					sd.tid = -1;
				} else
					sd.tid = tid;				
			} catch (Exception e) {
				logger.warning("Aborting import of line " + lineNumber + 
					", couldn't create type: '" + valueArray[4] + "'");
				continue;
			}
			if ( ds != null ) {
				if ( valueArrayLength > lineLen1 ) {
					try {
						sd.colid = columnMap.get( valueArray[7].trim() );
					} catch (Exception e) {
						logger.warning("Aborting import of line " + lineNumber + 
							", unknown column: '" + valueArray[7] + "'");
						continue;
					}
					if ( valueArrayLength < lineLen2 ) {
						sd.rid = defaultRank;
					} else {
						try {
							sd.rid = rankMap.get( valueArray[8].trim() );
						} catch (Exception e) {
							logger.warning("Aborting import of line " + lineNumber + 
								", unknown rank: '" + valueArray[8] + "'");
							continue;
						}
					}
				} else {
					sd.colid = -1;
					sd.rid = -1;
				}
			} else {
				sd.colid = -1;
				sd.rid = -1;
			}
			sd.name = valueArray[5].trim();
			sd.value = valueArray[6].trim();
			
			try {
				sd.sdid = Integer.parseInt( valueArray[0] );
			} catch (Exception e) {
				logger.warning("Aborting import of line " + lineNumber + 
					", unknown id: '" + valueArray[0] + "'");
				continue;
			}
			
			// Finally, insert/update the data
			try {
				if ( ds != null ) {
					if ( sd.tid == -1 ) {
						sd.tid = ds.insertSuppDataType( sd );
						if ( sd.tid == 0 ) {
							logger.warning("Aborting import of line " + lineNumber + 
								", problem inserting datatype" );
							continue;
						}
						sdtypeMap.put( sd.typeName, sd.tid );
						logger.info("Added supplemental datatype " + sd.typeName );
					}
					int read_sdid = sd.sdid;
					if ( sd.sdid == 0 ) {
						sd.sdid = ds.insertSuppDatum( sd );
					} else
						sd.sdid = ds.updateSuppDatum( sd );
					if ( sd.sdid < 0 ) {
						sd.sdid = -sd.sdid;
						logger.info("For import of line " + lineNumber + 
						", supp data record already exists as SDID " + sd.sdid +
						"; will create xref record");
					} else if ( sd.sdid==0 ) {
						logger.warning("Aborting import of line " + lineNumber + 
							", problem " + (read_sdid==0 ? "insert" : "updat") + "ing supp data" );
						continue;
					} else if ( read_sdid == 0 )
						logger.info("Added supp data record SDID " + sd.sdid);
					else
						logger.info("Updated supp data record SDID " + sd.sdid);
					if ( !ds.insertSuppDatumXref( sd ) )
						continue;
					else
						logger.info( "Added xref for SDID " + sd.sdid );
				} else {
					if ( sd.tid == -1 ) {
						sd.tid = vds.insertSuppDataType( sd );
						if ( sd.tid == 0 ) {
							logger.warning("Aborting import of line " + lineNumber + 
								", problem inserting datatype" );
							continue;
						}
						sdtypeMap.put( sd.typeName, sd.tid );
						logger.info("Added supplemental datatype " + sd.typeName );
					}
					int read_sdid = sd.sdid;
					if ( sd.sdid == 0 )
						sd.sdid = vds.insertSuppDatum( sd );
					else
						sd.sdid = vds.updateSuppDatum( sd );
					if ( sd.sdid < 0 ) {
						sd.sdid = -sd.sdid;
						logger.info("For import of line " + lineNumber + 
						", supp data record already exists as SDID " + sd.sdid +
						"; will create xref record");
					} else if ( sd.sdid==0 ) {
						logger.warning("Aborting import of line " + lineNumber + 
							", problem " + (read_sdid==0 ? "insert" : "updat") + "ing supp data" );
						continue;
					} else if ( read_sdid == 0 )
						logger.info("Added supp data record SDID " + sd.sdid);
					else
						logger.info("Updated supp data record SDID " + sd.sdid);
					if ( !vds.insertSuppDatumXref( sd ) )
						continue;
					else
						logger.info( "Added xref for SDID " + sd.sdid );
				}
			} catch (Exception e) {
				logger.warning("Failed import of line " + lineNumber + ", db failure: " + e);
				continue;
			}
			success++;
		}
		logger.info("" + success + " of " + lineNumber + " lines successfully processed");
	}
	
	/**
	 * Print usage.  Prints out usage instructions for the given importer
	 */
	public void outputInstructions(String importerClass, String message) {
	}
	
	public static void main(String as[]) {
		ImportSuppdata importer	= new ImportSuppdata();
		
		Arguments args = new Arguments(as, flags, keys);
		
		if (args.flagged("-h")) {
			importer.outputInstructions(importer.getClass().getName(), null);
			System.exit(-1);
		}
		
		String configFile = "VDX.config";
		if (args.contains("-c")) {
			configFile = args.get("-c");
		}

		importer.initialize(importer.getClass().getName(), configFile );
		List<String> files	= args.unused();
		for (String file : files) {
			importer.process(file);
		}
		
		importer.deinitialize();

	}	

}
