package gov.usgs.vdx.data.scheduler;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.Util;

import java.io.*;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

/**
*
* @author Loren Antolik
*/

//inner class, run method will be invoked when time is up
class SchedulerTask extends TimerTask {
	
	public void run() {
		
		// this is where we put the work that we want to run periodically
		System.out.println("i'm doing something ...");
	}
	
/*	
	public void whatever() {
		
		// get a list of all the files that we wish to process
		fileList	= ls sourceFileDirectory/sourceFileLookupExpression
		
		// for each one of the files, process it based on the import class
		foreach (currentFile in fileList) {
			
			// instatiate the class and process the file
			instantiate ImportClass(importParameters, currentFile)
			
			// archive the file if requested
			if (archiveProcessedFile) {
			}
			
			// rename the file if requested
			if (renameProcessedFile) {
			}
		}
		
	}
*/
}

public class Scheduler {
	
	// class variable declarations
	private static String		configFileName;
	private static ConfigFile	configFile;
	private static String		importClassName;
	private static List<String>	importParameters;
	private static String		sourceFileDirectoryName;
	private static String		sourceFileLookupExpression;
	private static int			pollingCycleSeconds;
	private static boolean		archiveProcessedFile;
	private static String		archiveDirectoryName;
	private static boolean		renameProcessedFile;
	private static String		renameValue;
	private static File			sourceFileDirectory;
	private static File			archiveDirectory;
	protected Logger			logger;
	private static Timer		timer;
	
	// constructor
	public Scheduler(String configFileName) {
		logger				= Log.getLogger("gov.usgs.vdx");
		configFile			= new ConfigFile(configFileName);
		processConfigFile();
		timer				= new Timer();
	}
	
	private void processConfigFile() {
		
		// read the config file
		importClassName				= Util.stringToString(configFile.getString("importClassName"),"");
		importParameters			= configFile.getList("importParameters");
		sourceFileDirectoryName		= Util.stringToString(configFile.getString("sourceFileDirectoryName"),"");
		sourceFileLookupExpression	= Util.stringToString(configFile.getString("sourceFileLookupExpression"),"");
		pollingCycleSeconds			= Util.stringToInt(configFile.getString("pollingCycleSeconds"), 3600);
		archiveProcessedFile		= Util.stringToBoolean(configFile.getString("archiveProcessedFile"));
		archiveDirectoryName		= Util.stringToString(configFile.getString("archiveDirectoryName"),"");
		renameProcessedFile			= Util.stringToBoolean(configFile.getString("renameProcessedFile"));
		renameValue					= Util.stringToString(configFile.getString("renameValue"),"");
		
		displayDefaultParameters("processConfigFile:read config file");
		
		// check to make sure that the class exists
		try {
			Class importClass	= Class.forName(importClassName);
		} catch (ClassNotFoundException e) {
			System.err.println("java gov.usgs.vdx.data.scheduler.Scheduler configFile");
			System.err.println("configFile:importClassName:ClassNotFoundException");
			System.exit(-1);			
		}
		
		// check to make sure that the source file directory exists
		sourceFileDirectory	= new File(sourceFileDirectoryName);
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
				
			// if we do have a value to rename the file to, then make sure the file is writable
			} else {
				
			}
			
		}		
		
		displayDefaultParameters("processConfigFile:validated input");
	}
	
	private void displayDefaultParameters(String currentState) {
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
		boolean exists	= (new File(configFileName)).isFile();
		if (!exists) {
			System.err.printf("%s does not exist\n", configFileName);
			System.exit(-1);
		}
		
		// instantiate this scheduler class by processing the config file and it's contents
		Scheduler schedule	= new Scheduler(configFileName);
		
		// the config file processed okay, so go ahead and start scheduling imports
		timer.scheduleAtFixedRate(new SchedulerTask(), 0, pollingCycleSeconds * 1000);
		
		// build a thread to run the program in
		// Process process			= new ProcessBuilder(args).start();
		// InputStream is			= process.getInputStream();
		// InputStreamReader isr	= new InputStreamReader(is);
		// BufferedReader br		= new BufferedReader(isr);
	
		// display program output
		// System.out.printf("Output of running %s is:\n", Arrays.toString(args));
		
		// display command output
		// while ((commandOutput = br.readLine()) != null) {
			// System.out.println(commandOutput);
		// }
	}
}