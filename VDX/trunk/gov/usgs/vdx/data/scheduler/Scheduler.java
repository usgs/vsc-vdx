package gov.usgs.vdx.data.scheduler;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.scheduler.CopyFile;

import java.io.*;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

/**
 * Scheduler is the main entry point for data acquisition importers.
 * it provides file management for data files, and an interface to 
 * the right importer for the job
 * 
 * @author Loren Antolik
 */


public class Scheduler {
	
	// config file variables
	private static String		importClassName;
	private static String		importParameters;
	private static String		sourceDirectoryName;
	private static String		sourceFileType;
	private static int			pollingCycleSeconds;
	private static boolean		archiveProcessedFile;
	private static String		archiveDirectoryName;
	private static boolean		deleteProcessedFile;
	
	// class variables
	private static String		configFileName;
	private static ConfigFile	configFile;
	private static Class		importClass;
	private static File			sourceDirectory;
	private static File			archiveDirectory;
	private static File			vdxDirectory;
	protected Logger			logger;
	
	/**
	* Constructor for no arguments (for use by a calling class)
	*/
	public Scheduler() {
	}
	
	/**
	*Constructor with command line arguments (for use from the command line)
	*/
	public Scheduler(String[] args) {
		init(args);
	}
	
	/**
	*Initialization method.  Initializes all values and runs the program
	*/
	public void init(String[] args) {
		
		// get the config file name and make sure that it exists
		configFileName	= args[0];
		configFile		= new ConfigFile(configFileName);
		if (!configFile.wasSuccessfullyRead()) {
			System.err.printf("%s was not successfully read.\n", configFileName);
			System.exit(-1);
		}
		
		// parse out the values from the config file
		parseConfigFileVals(configFile);
		
		// setup the proper data structures based on the default vals
		sourceDirectory				= new File(sourceDirectoryName);
		archiveDirectory			= new File(archiveDirectoryName);
		vdxDirectory				= new File("/hvo_cluster/software/vdx");
		logger						= Log.getLogger("gov.usgs.vdx");
		
		// validate the variables that were gathered from the config file
		validateSchedulerVars();
		
		// instantiate this scheduler class by processing the config file and it's contents
		Scheduler scheduler	= new Scheduler();
		Timer timer			= new Timer();
		
		// the config file processed okay, so go ahead and start scheduling imports
		timer.scheduleAtFixedRate(scheduler.new SchedulerTimerTask(), 0, pollingCycleSeconds * 1000);		
	}
	
	/**
	 * Parse configuration file and initialize internal variables
	 */
	public static void parseConfigFileVals(ConfigFile configFile) {
		
		// read the config file and give defaults
		importClassName				= Util.stringToString(configFile.getString("importClassName"),"");
		importParameters			= Util.stringToString(configFile.getString("importParameters"),"");
		sourceDirectoryName			= Util.stringToString(configFile.getString("sourceDirectoryName"),"");
		sourceFileType				= Util.stringToString(configFile.getString("sourceFileType"),"");
		pollingCycleSeconds			= Util.stringToInt(configFile.getString("pollingCycleSeconds"), 3600);
		archiveProcessedFile		= Util.stringToBoolean(configFile.getString("archiveProcessedFile"));
		archiveDirectoryName		= Util.stringToString(configFile.getString("archiveDirectoryName"),"");
		deleteProcessedFile			= Util.stringToBoolean(configFile.getString("deleteProcessedFile"));		
	}
	
	/**
	 * Check conditions for successful scheduled task execution
	 */
	public static void validateSchedulerVars() {
		
		// check to make sure that the class exists, don't instantiate it yet though
		try {
			importClass	= Class.forName(importClassName);
		} catch (ClassNotFoundException e) {
			System.err.println("configFile:importClassName:ClassNotFoundException");
			System.exit(-1);			
		}
		
		// check to make sure that the source file directory exists
		if (!sourceDirectory.isDirectory()) {
			System.err.println("configFile:sourceDirectoryName: Directory does not exist");
			System.exit(-1);
		
		// if it exists then make sure it is readable
		} else if (!sourceDirectory.canRead()){
			System.err.println("configFile:sourceDirectoryName: Directory is not readable");
			System.exit(-1);			
		}
		
		// check to make sure that there is a lookup expression
		if (sourceFileType.length() == 0) {
			System.err.println("configFile:sourceFileType: Not specified");
		}
		
		// check to make sure that there is a directive for archiving files
		if (archiveProcessedFile) {
			
			// if the directive is true then check to make sure that the directory to archive to exists
			if (!archiveDirectory.isDirectory()) {
				System.err.println("configFile:archiveDirectoryName: Directory does not exist");
				System.exit(-1);
				
			// if archive directory does exist then make sure it is writable
			} else if (!archiveDirectory.canWrite()) {
				System.err.println("configFile:archiveDirectoryName: Directory is not writeable");
				System.exit(-1);				
			}			
		}
	}
	
	/**
	 * Prints configuration parameters to stdout
	 */
	public static void displayDefaultParameters(String currentState) {
		System.out.println("--- Value : " + currentState + " ---");
		System.out.println("importClassName:            " + importClassName);
		System.out.println("importParameters:           " + importParameters);
		System.out.println("sourceDirectoryName:        " + sourceDirectoryName);
		System.out.println("sourceFileType:             " + sourceFileType);
		System.out.println("pollingCycleSeconds:        " + pollingCycleSeconds);
		System.out.println("archiveProcessedFile:       " + archiveProcessedFile);
		System.out.println("archiveDirectoryName:       " + archiveDirectoryName);
		System.out.println("deleteProcessedFile:        " + deleteProcessedFile);
	}
	
	/**
	*Main method
	*Syntax is:
	*java gov.usgs.vdx.data.scheduler.Scheduler configFile
	*/
	public static void main (String args[]) {
		
		// check to make sure there are command line arguments
		if (args.length != 1) {
			System.err.println("java gov.usgs.vdx.data.scheduler.Scheduler configFile");
			System.exit(-1);
		}
		
		Scheduler scheduler = new Scheduler(args);
	}
	
	/**Extension of class for getting the proper file listing
	 */
	class ImportFileFilter implements FileFilter {
		
		// constructor
		public ImportFileFilter () {
		}
		
		// inherited method
		public boolean accept(File file) {
			return file.getName().toLowerCase().endsWith(sourceFileType);
		}
	}
	
	/**Extension of class for timing the importing
	 */
	class SchedulerTimerTask extends TimerTask {
		
		// instance variables
		private ImportFileFilter		importFileFilter;
		private File[]					filesToProcess;
		private File					archiveFile;
		private String[]				arguments;
		private Importer				importer;
		
		// constructor
		public SchedulerTimerTask () {
			importFileFilter			= new ImportFileFilter();
		}
		
		// inherited method
		public void run(){
			
			String argString;
			
			// check for new files
			filesToProcess	= sourceDirectory.listFiles(importFileFilter);
			
			// if there are new file, then we can instantiate the class, thus creating a db connection
			// only try and make a db connection if we need one, no need having a connection open
			// all day long if we only use it for five seconds a day.
			if (filesToProcess.length > 0) {
				
				// instantiate the import class
				try {
					importClass	= Class.forName(importClassName);
				} catch (ClassNotFoundException e) {
					System.err.println("configFile:importClassName:ClassNotFoundException");
					System.exit(-1);			
				}
				
				// this will call the default constructor of the class
				try {
					importer	= (Importer)importClass.newInstance();
				} catch (InstantiationException e) {
					System.err.println("configFile:importClassName:InstantiationException");
					System.exit(-1);			
				} catch (IllegalAccessException e) {
					System.err.println("configFile:importClassName:IllegalAccessException");
					System.exit(-1);			
				}
				
				// for each of the files we are processing
				for (File file : filesToProcess) {					
					
					// add the file name to the import parameters list
					argString	= "";
					argString	= importParameters + " " + file.getAbsolutePath();
					arguments 	= argString.split(" ");

					// call the init function to add the data in the file
					importer.init(arguments);
			
					// archive the file if requested
					if (archiveProcessedFile) {
						System.out.println("archiving " + file.getAbsolutePath() + " to " + archiveDirectory);
						archiveFile	= new File(archiveDirectory, file.getName());
						CopyFile copyFile = new CopyFile();
						try {
							copyFile.copyFile(file, archiveFile);
						} catch (IOException e) {
							System.err.println("error copying file to archive directory");
						}
					}
			
					// rename the file if requested
					if (deleteProcessedFile) {
						System.out.println("deleting " + file.getAbsolutePath());
						if (!file.delete()) {
							System.err.println("error deleting source file " + file.getName());
						}
					}
				}			
			}
		}
	}
}