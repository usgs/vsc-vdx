package gov.usgs.vdx.data.scheduler;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.Util;

import java.io.*;
import java.lang.Class.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Logger;

/**
*
* @author Loren Antolik
*/
public class Scheduler {
	
	// class variable declarations
	private static String		configFileName;
	private static String		commandOutput;
	private static ConfigFile	configFile;
	private static String		importClassName;
	private static List<String>	importParameters;
	private static String		sourceFileDirectory;
	private static String		sourceFileLookupExpression;
	private static int			pollingCycleSeconds;
	private static boolean		archiveProcessedFile;
	private static String		archiveDirectory;
	private static boolean		renameProcessedFile;
	private static String		renameValue;
	protected Logger			logger;
	
	// constructor
	public Scheduler(String configFileName) {
		logger		= Log.getLogger("gov.usgs.vdx");
		configFile	= new ConfigFile(configFileName);
		processConfigFile();
	}
	
	private void processConfigFile() {
		
		// default some variables
		boolean exists;
		
		// read the config file
		importClassName				= Util.stringToString(configFile.getString("importClassName"),"");
		importParameters			= configFile.getList("importParameters");
		sourceFileDirectory			= Util.stringToString(configFile.getString("sourceFileDirectory"),"");
		sourceFileLookupExpression	= Util.stringToString(configFile.getString("sourceFileLookupExpression"),"");
		pollingCycleSeconds			= Util.stringToInt(configFile.getString("pollingCycleSeconds"), 3600);
		archiveProcessedFile		= Util.stringToBoolean(configFile.getString("archiveProcessedFile"));
		archiveDirectory			= Util.stringToString(configFile.getString("archiveDirectory"),"");
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
		exists	= (new File(sourceFileDirectory)).isDirectory();
		if (!exists) {
			System.err.println("configFile:sourceFileDirectory: Directory does not exist");
			System.exit(-1);
		}
		
		// check to make sure that there is a lookup expression
		if (sourceFileLookupExpression.length() == 0) {
			System.err.println("configFile:sourceFileLookupExpression: Not specified");
		}
		
		// check to make sure that there is a directive for archiving files
		if (archiveProcessedFile) {
			
			// if the directive is true then check to make sure that the directory to archive to exists
			exists	= (new File(archiveDirectory)).isDirectory();
			if (!exists) {
				System.err.println("configFile:archiveDirectory: Directory does not exist");
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
		
		displayDefaultParameters("processConfigFile:validated input");
	}
	
	private void process() {
		
	}
	
	private void displayDefaultParameters(String currentState) {
		System.out.println("--- Value : " + currentState + " ---");
		System.out.println("importClassName:            " + importClassName);
		System.out.println("importParameters:           " + importParameters);
		System.out.println("sourceFileDirectory:        " + sourceFileDirectory);
		System.out.println("sourceFileLookupExpression: " + sourceFileLookupExpression);
		System.out.println("pollingCycleSeconds:        " + pollingCycleSeconds);
		System.out.println("archiveProcessedFile:       " + archiveProcessedFile);
		System.out.println("archiveDirectory:           " + archiveDirectory);
		System.out.println("renameProcessedFile:        " + renameProcessedFile);
		System.out.println("renameValue:                " + renameValue);
	}
	
	// main class
	public static void main(String args[]) {
		
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
		
		// instantiate this scheduler class
		Scheduler schedule	= new Scheduler(configFileName);
		
		// call the process method
		// this method runs continously, doing the processing and timing work
		schedule.process();
		
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
