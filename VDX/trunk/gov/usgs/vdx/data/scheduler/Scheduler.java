package gov.usgs.vdx.data.scheduler;

import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.Util;

import java.io.*;
import java.util.*;
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
	//private static List<String>	importParameters;
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
	protected Logger			logger;
	private static File			vdxDirectory;
	
	// constructor
	public Scheduler() {
		logger				= Log.getLogger("gov.usgs.vdx");
		vdxDirectory		= new File("/hvo_cluster/software/vdx");
	}
	
	public static void parseConfigFileVals(ConfigFile configFile) {
		
		// read the config file and give defaults
		importClassName				= Util.stringToString(configFile.getString("importClassName"),"");
		// importParameters			= configFile.getList("importParameters");
		importParameters			= Util.stringToString(configFile.getString("importParameters"), "");
		sourceDirectoryName			= Util.stringToString(configFile.getString("sourceDirectoryName"),"");
		sourceFileType				= Util.stringToString(configFile.getString("sourceFileType"),"");
		pollingCycleSeconds			= Util.stringToInt(configFile.getString("pollingCycleSeconds"), 3600);
		archiveProcessedFile		= Util.stringToBoolean(configFile.getString("archiveProcessedFile"));
		archiveDirectoryName		= Util.stringToString(configFile.getString("archiveDirectoryName"),"");
		deleteProcessedFile			= Util.stringToBoolean(configFile.getString("deleteProcessedFile"));
		
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
		sourceDirectory				= new File(sourceDirectoryName);
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
			System.out.println("SchedulerFileFilter initialized with filter " + sourceFileType);
		}
		
		// inherited method
		public boolean accept(File file) {
			return file.getName().toLowerCase().endsWith(sourceFileType);
		}
	}
	
	//	 extension of class for timing the importing
	class SchedulerTimerTask extends TimerTask {
		
		// instance variables
		private SchedulerFileFilter		schedulerFileFilter;
		private File[]					filesToProcess;
		private String					fileName;
		private File					archiveFile;
		private Object					classObject;
		private ProcessBuilder			processBuilder;
		private Process					process;
		private String					command;
		private InputStream				is;
		private InputStreamReader		isr;
		private BufferedReader			br;
		private String 					line;
		private Map<String,String>		environment;
		
		// constructor
		public SchedulerTimerTask () {
			schedulerFileFilter			= new SchedulerFileFilter();
		}
		
		// inherited method
		public void run(){
			
			// check for new files
			filesToProcess	= sourceDirectory.listFiles(schedulerFileFilter);
			
			// if there are new file
			if (filesToProcess.length > 0) {
				
				// set up the process builder
				processBuilder	= new ProcessBuilder();
				processBuilder.directory(vdxDirectory);
				
				environment			= processBuilder.environment();
				Set keys			= environment.keySet(); 
			    Iterator keyIter	= keys.iterator();
			    System.out.println("The map contains the following associations:");
			    while (keyIter.hasNext()) {
			    	Object key = keyIter.next();
			    	Object value = environment.get(key);
			    	System.out.println( "   (" + key + "," + value + ")" );
			    }
				
				// lets see what we are running as
				try {
					command = "pwd";
					processBuilder.command(command);
					process = processBuilder.start();
					is		= process.getInputStream();
					isr		= new InputStreamReader(is);
					br		= new BufferedReader(isr);
					System.out.println("pwd");
					while ((line = br.readLine()) != null) {
						System.out.println(line);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				// instantiate the import class
				// try {
					// importClass	= Class.forName(importClassName);
				// } catch (ClassNotFoundException e) {
					// System.err.println("configFile:importClassName:ClassNotFoundException");
					// System.exit(-1);			
				// }
				
				// this will call the default constructor of the class
				// try {
					// classObject	= importClass.newInstance();
				// } catch (InstantiationException e) {
					// System.err.println("configFile:importClassName:InstantiationException");
					// System.exit(-1);			
				// } catch (IllegalAccessException e) {
					// System.err.println("configFile:importClassName:IllegalAccessException");
					// System.exit(-1);			
				// }
				
				// for each of the files we are processing
				for (File file : filesToProcess) {
					
					// define the command for the process builder					
					try {
						command		= "/usr/java/default/bin/java " +
						              "-cp lib/usgs.jar:contrib/mysql.jar:contrib/colt.jar " +
						              importClassName + " " + 
						              importParameters + " " +
						              file.getAbsolutePath();
						processBuilder.command(command);
						process		= processBuilder.start();	
						process.waitFor();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					// importClass.process(file)
			
					// archive the file if requested
					if (archiveProcessedFile) {
						System.out.println("archiving " + file.getAbsolutePath());
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
			
				// close the class and the db connection
				// class.close();
			
			}
		}
	}
}