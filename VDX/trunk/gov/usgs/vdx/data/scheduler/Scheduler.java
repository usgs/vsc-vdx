package gov.usgs.vdx.data.scheduler;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.Util;

import java.io.*;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

/*
* @author Loren Antolik
*/

public class Scheduler {
	
	// config file variables
	private static String		importClassName;
	private static List<String>	importParameters;
	private static String		sourceFileDirectoryName;
	private static String		sourceFileLookupExpression;
	private static int			pollingCycleSeconds;
	private static boolean		archiveProcessedFile;
	private static String		archiveDirectoryName;
	private static boolean		renameProcessedFile;
	private static String		renameValue;
	
	// class variables
	private static String		configFileName;
	private static ConfigFile	configFile;
	private static Class		importClass;
	private static File			sourceFileDirectory;
	private static File			archiveDirectory;
	protected Logger			logger;
	
	// constructor
	public Scheduler() {
		logger				= Log.getLogger("gov.usgs.vdx");
	}
	
	public static void parseConfigFileVals(ConfigFile configFile) {
		
		// read the config file and give defaults
		importClassName				= Util.stringToString(configFile.getString("importClassName"),"");
		importParameters			= configFile.getList("importParameters");
		sourceFileDirectoryName		= Util.stringToString(configFile.getString("sourceFileDirectoryName"),"");
		sourceFileLookupExpression	= Util.stringToString(configFile.getString("sourceFileLookupExpression"),"");
		pollingCycleSeconds			= Util.stringToInt(configFile.getString("pollingCycleSeconds"), 3600);
		archiveProcessedFile		= Util.stringToBoolean(configFile.getString("archiveProcessedFile"));
		archiveDirectoryName		= Util.stringToString(configFile.getString("archiveDirectoryName"),"");
		renameProcessedFile			= Util.stringToBoolean(configFile.getString("renameProcessedFile"));
		renameValue					= Util.stringToString(configFile.getString("renameValue"),"");
		
	}
	
	public static void validateSchedulerVars() {
		
		// check to make sure that the class exists, don't instantiate it yet though
		try {
			importClass	= Class.forName(importClassName);
		} catch (ClassNotFoundException e) {
			System.err.println("configFile:importClassName:ClassNotFoundException");
			System.exit(-1);			
		}
		
		// check to make sure that the source file directory exists
		if (!sourceFileDirectory.isDirectory()) {
			System.err.println("configFile:sourceFileDirectoryName: Directory does not exist");
			System.exit(-1);
		
		// if it exists then make sure it is readable
		} else if (!sourceFileDirectory.canRead()){
			System.err.println("configFile:sourceFileDirectoryName: Directory is not readable");
			System.exit(-1);			
		}
		
		// check to make sure that there is a lookup expression
		if (sourceFileLookupExpression.length() == 0) {
			System.err.println("configFile:sourceFileLookupExpression: Not specified");
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
		
		// check to make sure that there is a directive for renaming files
		if (renameProcessedFile) {
			
			// if the directive is true then check to make sure that the rename mask is set
			if (renameValue.length() == 0) {
				System.err.println("configFile:renameValue: Not specified");
			}			
		}	
	}
	
	public static void displayDefaultParameters(String currentState) {
		System.out.println("--- Value : " + currentState + " ---");
		System.out.println("importClassName:            " + importClassName);
		System.out.println("importParameters:           " + importParameters);
		System.out.println("sourceFileDirectoryName:    " + sourceFileDirectoryName);
		System.out.println("sourceFileLookupExpression: " + sourceFileLookupExpression);
		System.out.println("pollingCycleSeconds:        " + pollingCycleSeconds);
		System.out.println("archiveProcessedFile:       " + archiveProcessedFile);
		System.out.println("archiveDirectoryName:       " + archiveDirectoryName);
		System.out.println("renameProcessedFile:        " + renameProcessedFile);
		System.out.println("renameValue:                " + renameValue);
	}
	
	// main class
	public static void main (String args[]) {
		
		// check to make sure there are command line arguments
		if (args.length != 1) {
			System.err.println("java gov.usgs.vdx.data.scheduler.Scheduler configFile");
			System.exit(-1);
		}
		
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
		sourceFileDirectory			= new File(sourceFileDirectoryName);
		archiveDirectory			= new File(archiveDirectoryName);
		
		// validate the variables that were gathered from the config file
		validateSchedulerVars();
		
		// instantiate this scheduler class by processing the config file and it's contents
		Scheduler scheduler	= new Scheduler();
		Timer timer			= new Timer();
		
		// the config file processed okay, so go ahead and start scheduling imports
		timer.scheduleAtFixedRate(scheduler.new SchedulerTimerTask(), 0, pollingCycleSeconds * 1000);
	}
	
	// extension of class for getting the proper file listing
	class SchedulerFileFilter implements FileFilter {
		
		// constructor
		public SchedulerFileFilter () {
			System.out.println("SchedulerFileFilter initialized with filter " + sourceFileLookupExpression);
		}
		
		// inherited method
		public boolean accept(File file) {
			return file.getName().toLowerCase().endsWith(sourceFileLookupExpression);
		}
	}
	
	//	 extension of class for timing the importing
	class SchedulerTimerTask extends TimerTask {
		
		// instance variables
		private SchedulerFileFilter		schedulerFileFilter;
		private File[]					filesToProcess;
		
		// constructor
		public SchedulerTimerTask () {
			schedulerFileFilter			= new SchedulerFileFilter();
		}
		
		// inherited method
		public void run() {
			
			// check for new files
			filesToProcess	= sourceFileDirectory.listFiles(schedulerFileFilter);
			
			// if there are new file
			if (filesToProcess != null) {
				
				// check to make sure that the class exists, don't instantiate it yet though
				try {
					importClass	= Class.forName(importClassName);
				} catch (ClassNotFoundException e) {
					System.err.println("configFile:importClassName:ClassNotFoundException");
					System.exit(-1);			
				}
				
				// for each of the files we are processing
				for (File file : filesToProcess) {
					
					// this is where we put the work that we want to run periodically
					System.out.println(file.getAbsolutePath());
			
					// process the file through the import classes process method
					// importClass.process(file)
			
					// archive the file if requested
					if (archiveProcessedFile) {
						
					}
			
					// rename the file if requested
					if (renameProcessedFile) {
						
					}
				}
			
				// close the class and the db connection
				// class.close();
			
			}
		}
	}
}