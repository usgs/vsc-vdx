package gov.usgs.vdx.in;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.CurrentTime;
import gov.usgs.util.Util;
import gov.usgs.util.FileCopy;

import java.io.*;
import java.text.Collator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Scheduler is the main entry point for data acquisition importers.
 * it provides file management for data files, and an interface to 
 * the right importer for the job
 * 
 * @author Loren Antolik
 */


public class Scheduler {
	
	public static Set<String> flags;
	public static Set<String> keys;	
	public static Logger logger;
	public static ConfigFile params;
	public static ConfigFile schedulerParams;	
	public static Class importClass;	
	public CurrentTime currentTime = CurrentTime.getInstance();	

	public static File dataDir;
	public static File configFile;
	public static File archiveDir;
	
	// config file variables
	public static String	configDirectoryName;
	public static String	dataDirectoryName;
	public static String	archiveDirectoryName;
	
	public static String	importerName;
	public static String	dataDirName;
	public static String	configFileName;
	public static String	archiveDirName;
	public static String	fileSuffix;
	public static String	filePrefix;
	public static int		cycle;
	public static boolean	archive;
	public static boolean	delete;
	public static boolean	verbose;
	
	static {
		flags	= new HashSet<String>();
		keys	= new HashSet<String>();
		keys.add("-c");
		keys.add("-n");
		flags.add("-h");
		flags.add("-v");
	}
	
	/**
	*Initialization method.  Initializes all values and runs the program
	*/
	public void initialize(String importerClass, String schedulerConfigFile, String schedulerName) {
		
		// initialize the logger for this importer
		logger	= Logger.getLogger(importerClass);
		
		// parse out the values from the config file
		processConfigFile(schedulerConfigFile, schedulerName);
		
		// output some information about this configuration
		logger.log(Level.INFO, "importer:   " + importerName);
		logger.log(Level.INFO, "cycle:      " + cycle);
		logger.log(Level.INFO, "fileprefix: " + filePrefix);
		logger.log(Level.INFO, "filesuffix: " + fileSuffix);
		logger.log(Level.INFO, "delete:     " + delete);
		logger.log(Level.INFO, "verbose:    " + verbose);
		logger.log(Level.INFO, "archive:    " + archive);
		logger.log(Level.INFO, "datadir:    " + dataDir.getAbsolutePath());
		logger.log(Level.INFO, "configfile: " + configFile.getAbsolutePath());
		if (archive) { logger.log(Level.INFO, "archivedir: " + archiveDir.getAbsolutePath()); }
		
		// instantiate this scheduler class by processing the config file and it's contents
		Scheduler scheduler	= new Scheduler();
		Timer timer			= new Timer();
		
		// setup a logger to the log file
		/*
		try {
			fileHandler = new FileHandler(logFile.getAbsolutePath());
			fileHandler.setFormatter(new SimpleFormatter());
			logger.addHandler(fileHandler);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "unable to attach logger to log file");
		}
		*/
		
		// the config file processed okay, so go ahead and start scheduling imports
		timer.scheduleAtFixedRate(scheduler.new SchedulerTimerTask(), 0, cycle * 1000);
	}
	
	/**
	 * Parse configuration file and initialize internal variables
	 */
	public void processConfigFile(String schedulerConfigFile, String schedulerName) {
		
		// get the config file name and make sure that it exists
		params		= new ConfigFile(schedulerConfigFile);
		if (!params.wasSuccessfullyRead()) {
			logger.log(Level.SEVERE, schedulerConfigFile + " was not successfully read");
			System.exit(-1);
		}
		
		// get configuration wide params
		configDirectoryName		= params.getString("configDirectory");
		dataDirectoryName		= params.getString("dataDirectory");
		archiveDirectoryName	= params.getString("archiveDirectory");
		
		// get scheduler specific params
		schedulerParams		= params.getSubConfig(schedulerName);
		importerName		= schedulerParams.getString("importer");
		dataDirName			= schedulerParams.getString("dataDir");
		configFileName		= schedulerParams.getString("configFile");
		archiveDirName		= schedulerParams.getString("archiveDir");
		
		filePrefix		= Util.stringToString(schedulerParams.getString("filePrefix"), "");
		fileSuffix		= Util.stringToString(schedulerParams.getString("fileSuffix"), "");
		cycle			= Util.stringToInt(schedulerParams.getString("cycle"), 3600);
		archive			= Util.stringToBoolean(schedulerParams.getString("archive"), false);
		delete			= Util.stringToBoolean(schedulerParams.getString("delete"), false);
		verbose			= Util.stringToBoolean(schedulerParams.getString("verbose"), true);
		
		// validate the importer name
		if (importerName == null) {
			logger.log(Level.SEVERE, "importer parameter empty");
			System.exit(-1);
		} else {
			try {
				importClass	= Class.forName(importerName);
			} catch (ClassNotFoundException e) {
				logger.log(Level.SEVERE, "importer not found");
				System.exit(-1);			
			}
		}

		if (dataDirName == null) {
			logger.log(Level.SEVERE, "dataDir parameter empty");
			System.exit(-1);
		} else {
			if (dataDirName.indexOf("DATA_DIR") > -1) {
				if (dataDirectoryName != null) {
					dataDirName = dataDirName.replaceAll("DATA_DIR", dataDirectoryName);
				} else {
					logger.log(Level.SEVERE, "dataDirectory parameter empty");
					System.exit(-1);
				}
			}
		}
		
		// re-assign full directory names if variables were used
		if (configFileName == null) {
			logger.log(Level.SEVERE, "configFile parameter empty");
			System.exit(-1);
		} else {
			if (configFileName.indexOf("CONFIG_DIR") > -1) {
				if (configDirectoryName != null) {
					configFileName = configFileName.replaceAll("CONFIG_DIR", configDirectoryName);
				} else {
					logger.log(Level.SEVERE, "configDirectory parameter empty");
					System.exit(-1);
				}
			}
		}
		
		if (archive) {
			if (archiveDirName == null) {
				logger.log(Level.SEVERE, "archiveDir parameter empty");
				System.exit(-1);
			} else {
				if (archiveDirName.indexOf("ARCHIVE_DIR") > -1) {
					if (archiveDirectoryName != null) {
						archiveDirName = archiveDirName.replaceAll("ARCHIVE_DIR", archiveDirectoryName);
					} else {
						logger.log(Level.SEVERE, "archiveDirectory parameter empty");
						System.exit(-1);
					}
				}
			}
		}
		
		// setup the proper data structures based on the default vals
		dataDir		= new File(dataDirName);
		configFile	= new File(configFileName);
		if (archive) {
			archiveDir	= new File(archiveDirName);
		}
		
		if (!dataDir.isDirectory()) {
			logger.log(Level.SEVERE, dataDirName + " does not exist");
			System.exit(-1);
		} else if (!dataDir.canRead()) {
			logger.log(Level.SEVERE, dataDirName + " is not readable");
			System.exit(-1);
		}
		
		if (!configFile.exists()) {
			logger.log(Level.SEVERE, configFileName + " does not exist");
			System.exit(-1);
		} else if (!configFile.canRead()) {
			logger.log(Level.SEVERE, configFileName + " is not readable");
			System.exit(-1);
		}
		
		if (archive) {
			if (!archiveDir.isDirectory()) {
				logger.log(Level.SEVERE, archiveDirName + " does not exist");
				System.exit(-1);
			} else if (!archiveDir.canRead()) {
				logger.log(Level.SEVERE, archiveDirName + " is not readable");
				System.exit(-1);
			} else if (!archiveDir.canWrite()) {
				logger.log(Level.SEVERE, archiveDirName + " is not writable");
				System.exit(-1);
			}
		}
	}
	
	public void outputInstructions(String className, String message) {
		if (message == null) {
			System.err.println(message);
		}
		System.err.println(className + " -c configfile -n schedulername");
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
		
		Scheduler scheduler	= new Scheduler();
		
		Arguments args = new Arguments(as, flags, keys);
		
		if (args.flagged("-h")) {
			scheduler.outputInstructions(scheduler.getClass().getName(), null);
			System.exit(-1);
		}
		
		if (!args.contains("-c")) {
			scheduler.outputInstructions(scheduler.getClass().getName(), "config file required");
			System.exit(-1);
		}
		
		if (!args.contains("-n")) {
			scheduler.outputInstructions(scheduler.getClass().getName(), "scheduler name required");
			System.exit(-1);
		}

		scheduler.initialize(scheduler.getClass().getName(), args.get("-c"), args.get("-n"));
	}	
	
	/**Extension of class for getting the proper file listing
	 */
	class ImportFileFilter implements FileFilter {
		
		// constructor
		public ImportFileFilter () {
		}
		
		// inherited method
		public boolean accept(File file) {
			boolean prefixMatches = file.getName().startsWith(filePrefix);
			boolean suffixMatches = file.getName().endsWith(fileSuffix);
			if (prefixMatches && suffixMatches) {
				return true;
			} else {
				return false;
			}
		}
	}
	
	/**Extension of class for timing the importing
	 */
	class SchedulerTimerTask extends TimerTask {
		
		// instance variables
		private ImportFileFilter		importFileFilter;
		private File[]					selectedFiles;
		private File					archiveFile;
		private Importer				importer;
		
		// constructor
		public SchedulerTimerTask () {
			importFileFilter			= new ImportFileFilter();
		}
		
		// inherited method
		public void run(){
			
			// check for new files
			selectedFiles	= dataDir.listFiles(importFileFilter);
			
			// sort the array by filename
			Arrays.sort(selectedFiles, new FileComparator());
			
			// output information related to this scheduler
			logger.log(Level.INFO, "");
			logger.log(Level.INFO, currentTime.nowString() + " file count:" + selectedFiles.length);
			
			// if there are new file, then we can instantiate the class, thus creating a db connection
			// only try and make a db connection if we need one, no need having a connection open
			// all day long if we only use it for five seconds a day.
			if (selectedFiles.length > 0) {
				
				// instantiate the import class
				try {
					importClass	= Class.forName(importerName);
				} catch (ClassNotFoundException e) {
					logger.log(Level.SEVERE, "importer not found");
					System.exit(-1);			
				}
				
				// this will call the default constructor of the class
				try {
					importer	= (Importer)importClass.newInstance();
				} catch (InstantiationException e) {
					logger.log(Level.SEVERE, importerName + " InstantiationException");
					System.exit(-1);			
				} catch (IllegalAccessException e) {
					logger.log(Level.SEVERE, importerName + " IllegalAccessException");
					System.exit(-1);			
				}
				
				// initialize the importer
				importer.initialize(importerName, configFileName, verbose);
				
				// for each of the files we are processing
				for (File file : selectedFiles) {
					
					// process this file through the importer
					importer.process(file.getAbsolutePath());
			
					// archive the file if requested
					if (archive) {
						logger.log(Level.INFO, "archiving " + file.getAbsolutePath() + " to " + archiveDir.getAbsolutePath());
						archiveFile	= new File(archiveDir, file.getName());
						try {
							FileCopy.fileCopy(file, archiveFile);
						} catch (IOException e) {
							logger.log(Level.SEVERE, "error copying file to archive directory");
						}
					}
			
					// rename the file if requested
					if (delete) {
						logger.log(Level.INFO, "deleting " + file.getAbsolutePath());
						if (!file.delete()) {
							logger.log(Level.SEVERE, "error deleting " + file.getName());
						}
					}
				}
				
				// de-initialize the importer
				importer.deinitialize();
			}
		}
	}
	
	private static class FileComparator implements Comparator {
		
		private Collator c = Collator.getInstance();
		
		public int compare(Object o1, Object o2) {
			if(o1 == o2)
				return 0;

			File f1 = (File) o1;
			File f2 = (File) o2;

			if(f1.isDirectory() && f2.isFile())
				return -1;
			if(f1.isFile() && f2.isDirectory())
				return 1;

			return c.compare(f1.getName(), f2.getName());
		}
	}
}