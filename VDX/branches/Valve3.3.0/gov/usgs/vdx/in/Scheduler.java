package gov.usgs.vdx.in;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Util;
import gov.usgs.util.FileCopy;

import java.io.*;
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
	
	public Logger logger;
	
	public ConfigFile params;
	public ConfigFile schedulerParams;
	
	public Class importClass;

	public File dataDir;
	public File	configFile;
	public File logFile;
	public File archiveDir;
	
	// config file variables
	public String		configDirectoryName;
	public String		dataDirectoryName;
	public String		logDirectoryName;
	public String		archiveDirectoryName;
	
	public String		importerName;
	public String		dataDirName;
	public String		configFileName;
	public String		logFileName;
	public String		archiveDirName;
	public String		fileSuffix;
	public int			cycle;
	public boolean		archive;
	public boolean		delete;
	public boolean		verbose;
	
	static {
		flags	= new HashSet<String>();
		keys	= new HashSet<String>();
		keys.add("-c");
		flags.add("-h");
		flags.add("-v");
	}
	
	/**
	*Initialization method.  Initializes all values and runs the program
	*/
	public void initialize(String importerClass, String schedulerConfigFile, String schedulerName) {
		
		// initialize the logger for this importer
		logger	= Logger.getLogger(importerClass);
		logger.log(Level.INFO, "Scheduler.initialize() succeeded.");
		
		// parse out the values from the config file
		processConfigFile(schedulerConfigFile, schedulerName);
		
		// instantiate this scheduler class by processing the config file and it's contents
		Scheduler scheduler	= new Scheduler();
		Timer timer			= new Timer();
		
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
		logDirectoryName		= params.getString("logDirectory");
		archiveDirectoryName	= params.getString("archiveDirectory");
		
		// get scheduler specific params
		schedulerParams		= params.getSubConfig(schedulerName);
		importerName		= schedulerParams.getString("importer");
		dataDirName			= schedulerParams.getString("dataDir");
		configFileName		= schedulerParams.getString("configFile");
		logFileName			= schedulerParams.getString("logFile");
		archiveDirName		= schedulerParams.getString("archiveDir");
		
		fileSuffix		= Util.stringToString(schedulerParams.getString("fileSuffix"), "");
		cycle			= Util.stringToInt(schedulerParams.getString("cycle"), 3600);
		archive			= Util.stringToBoolean(schedulerParams.getString("archive"), false);
		delete			= Util.stringToBoolean(schedulerParams.getString("delete"), true);
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

		if (logFileName == null) {
			logger.log(Level.SEVERE, "logFile parameter empty");
			System.exit(-1);
		} else {
			if (logFileName.indexOf("LOG_DIR") > -1) {
				if (logDirectoryName != null) {
					logFileName = logFileName.replaceAll("LOG_DIR", logDirectoryName);
				} else {
					logger.log(Level.SEVERE, "logDirectory parameter empty");
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
		logFile		= new File(logFileName);
		if (archive) {
			archiveDir	= new File(archiveDirName);
		}
		
		if (!dataDir.isDirectory()) {
			logger.log(Level.SEVERE, "dataDir " + dataDirName + " does not exist");
			System.exit(-1);
		} else if (!dataDir.canRead()) {
			logger.log(Level.SEVERE, "dataDir " + dataDirName + " is not readable");
			System.exit(-1);
		}		
		
		if (!configFile.exists()) {
			logger.log(Level.SEVERE, "configFile " + configFileName + " does not exist");
			System.exit(-1);
		} else if (!configFile.canRead()) {
			logger.log(Level.SEVERE, "configFile " + configFileName + " is not readable");
			System.exit(-1);
		}
		
		if (logFile.exists()) {			
			try {
				String logFileNameCopy	= logFileName = ".sav";
				File logFileCopy		= new File(logFileNameCopy);
				logFileCopy.delete();
				FileCopy.fileCopy(logFile, logFileCopy);
				logger.log(Level.INFO, logFileName + " exists.  saving as " + logFileCopy);
			} catch (IOException e) {
				System.err.println("error copying file to archive directory");
			}
			if (logFile.delete()) {
				logFile	= new File(logFileName);
			}
		}
			
		if (!logFile.canWrite()) {
			logger.log(Level.SEVERE, "logFile " + logFileName + " is not writable");
			System.exit(-1);
		}
		
		if (archive) {
			if (!archiveDir.isDirectory()) {
				logger.log(Level.SEVERE, "archiveDir " + archiveDirName + " does not exist");
				System.exit(-1);
			} else if (!archiveDir.canRead()) {
				logger.log(Level.SEVERE, "archiveDir " + archiveDirName + " is not readable");
				System.exit(-1);
			} else if (!archiveDir.canWrite()) {
				logger.log(Level.SEVERE, "archiveDir " + archiveDirName + " is not writable");
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
			return file.getName().toLowerCase().endsWith(fileSuffix);
		}
	}
	
	/**Extension of class for timing the importing
	 */
	class SchedulerTimerTask extends TimerTask {
		
		// instance variables
		private ImportFileFilter		importFileFilter;
		private File[]					fileArray;
		private File					archiveFile;
		private Importer				importer;
		
		// constructor
		public SchedulerTimerTask () {
			importFileFilter			= new ImportFileFilter();
		}
		
		// inherited method
		public void run(){
			
			// check for new files
			fileArray	= dataDir.listFiles(importFileFilter);
			
			// if there are new file, then we can instantiate the class, thus creating a db connection
			// only try and make a db connection if we need one, no need having a connection open
			// all day long if we only use it for five seconds a day.
			if (fileArray.length > 0) {
				
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
				
				// for each of the files we are processing
				for (File file : fileArray) {

					// call the init function to setup the importer
					importer.initialize(importerName, configFileName, verbose);
					
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
			}
		}
	}
}