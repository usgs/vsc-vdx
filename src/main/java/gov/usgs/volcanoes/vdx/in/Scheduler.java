package gov.usgs.volcanoes.vdx.in;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.legacy.Arguments;
import gov.usgs.volcanoes.core.legacy.util.FileCopy;
import gov.usgs.volcanoes.core.time.CurrentTime;
import gov.usgs.volcanoes.core.time.Time;
import gov.usgs.volcanoes.core.util.StringUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.text.Collator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduler is the main entry point for data acquisition importers. it provides file management for
 * data files, and an interface to the right importer for the job
 *
 * @author Loren Antolik
 * @author Bill Tollett
 */


public class Scheduler {

  private static Logger LOGGER = LoggerFactory.getLogger(Scheduler.class);

  // class variables
  public static final Set<String> flags;
  public static final Set<String> keys;
  public static ConfigFile params;
  public static ConfigFile schedulerParams;
  public static Class importClass;

  public static File dataDir;
  public static File configFile;
  public static File archiveDir;

  // config file variables
  public static String configDirectoryName;
  public static String dataDirectoryName;
  public static String archiveDirectoryName;

  public static String importerName;
  public static String dataDirName;
  public static String configFileName;
  public static String archiveDirName;
  public static String fileSuffix;
  public static String filePrefix;
  public static int cycle;
  public static boolean archive;
  public static boolean delete;
  public static boolean verbose;

  // timing output
  public CurrentTime currentTime = CurrentTime.getInstance();

  static {
    flags = new HashSet<String>();
    keys = new HashSet<String>();
    keys.add("-c");
    keys.add("-n");
    flags.add("-h");
    flags.add("-v");
  }

  /**
   * Initialization method.  Initializes all values and runs the program
   *
   * @param importerClass name of importer class
   * @param schedulerConfigFile configuration file
   * @param schedulerName name of scheduler
   */
  public void initialize(String importerClass, String schedulerConfigFile, String schedulerName) {

    // initialize the LOGGER for this importer
    LOGGER.info("Scheduler.initialize() succeeded.");

    // parse out the values from the config file
    processConfigFile(schedulerConfigFile, schedulerName);

    // output some information about this configuration
    LOGGER.info("importer:   {}", importerName);
    LOGGER.info("cycle:      {}", cycle);
    LOGGER.info("fileprefix: {}", filePrefix);
    LOGGER.info("filesuffix: {}", fileSuffix);
    LOGGER.info("delete:     {}", delete);
    LOGGER.info("verbose:    {}", verbose);
    LOGGER.info("archive:    {}", archive);
    LOGGER.info("datadir:    {}", dataDir.getAbsolutePath());
    LOGGER.info("configfile: {}", configFile.getAbsolutePath());
    if (archive) {
      LOGGER.info("archivedir: {}", archiveDir.getAbsolutePath());
    }

    // instantiate this scheduler class by processing the config file and it's contents
    Scheduler scheduler = new Scheduler();
    Timer timer = new Timer();

    // the config file processed okay, so go ahead and start scheduling imports
    timer.scheduleAtFixedRate(scheduler.new SchedulerTimerTask(), 0, cycle * 1000);
  }

  /**
   * Parse configuration file and initialize internal variables.
   *
   * @param schedulerConfigFile configuration file
   * @param schedulerName name of scheduler
   */
  public void processConfigFile(String schedulerConfigFile, String schedulerName) {

    // get the config file name and make sure that it exists
    params = new ConfigFile(schedulerConfigFile);
    if (!params.wasSuccessfullyRead()) {
      LOGGER.error("{} was not successfully read", schedulerConfigFile);
      System.exit(-1);
    }

    // get configuration wide params
    configDirectoryName = params.getString("configDirectory");
    dataDirectoryName = params.getString("dataDirectory");
    archiveDirectoryName = params.getString("archiveDirectory");

    // get scheduler specific params
    schedulerParams = params.getSubConfig(schedulerName);
    importerName = schedulerParams.getString("importer");
    dataDirName = schedulerParams.getString("dataDir");
    configFileName = schedulerParams.getString("configFile");
    archiveDirName = schedulerParams.getString("archiveDir");

    filePrefix = StringUtils.stringToString(schedulerParams.getString("filePrefix"), "");
    fileSuffix = StringUtils.stringToString(schedulerParams.getString("fileSuffix"), "");
    cycle = StringUtils.stringToInt(schedulerParams.getString("cycle"), 3600);
    archive = StringUtils.stringToBoolean(schedulerParams.getString("archive"), false);
    delete = StringUtils.stringToBoolean(schedulerParams.getString("delete"), false);
    verbose = StringUtils.stringToBoolean(schedulerParams.getString("verbose"), true);

    // validate the importer name
    if (importerName == null) {
      LOGGER.error("importer parameter empty");
      System.exit(-1);
    } else {
      try {
        importClass = Class.forName(importerName);
      } catch (ClassNotFoundException e) {
        LOGGER.error("importer not found");
        System.exit(-1);
      }
    }

    if (dataDirName == null) {
      LOGGER.error("dataDir parameter empty");
      System.exit(-1);
    } else {
      if (dataDirName.indexOf("DATA_DIR") > -1) {
        if (dataDirectoryName != null) {
          dataDirName = dataDirName.replaceAll("DATA_DIR", dataDirectoryName);
        } else {
          LOGGER.error("dataDirectory parameter empty");
          System.exit(-1);
        }
      }
    }

    // re-assign full directory names if variables were used
    if (configFileName == null) {
      LOGGER.error("configFile parameter empty");
      System.exit(-1);
    } else {
      if (configFileName.indexOf("CONFIG_DIR") > -1) {
        if (configDirectoryName != null) {
          configFileName = configFileName.replaceAll("CONFIG_DIR", configDirectoryName);
        } else {
          LOGGER.error("configDirectory parameter empty");
          System.exit(-1);
        }
      }
    }

    if (archive) {
      if (archiveDirName == null) {
        LOGGER.error("archiveDir parameter empty");
        System.exit(-1);
      } else {
        if (archiveDirName.indexOf("ARCHIVE_DIR") > -1) {
          if (archiveDirectoryName != null) {
            archiveDirName = archiveDirName.replaceAll("ARCHIVE_DIR", archiveDirectoryName);
          } else {
            LOGGER.error("archiveDirectory parameter empty");
            System.exit(-1);
          }
        }
      }
    }

    // setup the proper data structures based on the default vals
    dataDir = new File(dataDirName);
    configFile = new File(configFileName);
    if (archive) {
      archiveDir = new File(archiveDirName);
    }

    if (!dataDir.isDirectory()) {
      LOGGER.error("{} does not exist", dataDirName);
      System.exit(-1);
    } else if (!dataDir.canRead()) {
      LOGGER.error("{} is not readable", dataDirName);
      System.exit(-1);
    }

    if (!configFile.exists()) {
      LOGGER.error("{} does not exist", configFileName);
      System.exit(-1);
    } else if (!configFile.canRead()) {
      LOGGER.error("{} is not readable", configFileName);
      System.exit(-1);
    }

    if (archive) {
      if (!archiveDir.isDirectory()) {
        LOGGER.error("{} does not exist", archiveDirName);
        System.exit(-1);
      } else if (!archiveDir.canRead()) {
        LOGGER.error("{} is not readable", archiveDirName);
        System.exit(-1);
      } else if (!archiveDir.canWrite()) {
        LOGGER.error("{} is not writable", archiveDirName);
        System.exit(-1);
      }
    }
  }

  /**
   * Print usage.  Prints out usage instructions for the scheduler
   *
   * @param className name of class
   * @param message instructions
   */
  public void outputInstructions(String className, String message) {
    if (message == null) {
      System.err.println(message);
    }
    System.err.println(className + " -c configfile -n schedulername");
  }

  /**
   * Main method. Command line syntax: -h, --help print help message -c config file name -v verbose
   * mode files ...
   *
   * @param as command line args
   */
  public static void main(String[] as) {

    Scheduler scheduler = new Scheduler();

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

  /**
   * Extension of class for getting the proper file listing.
   */
  static class ImportFileFilter implements FileFilter {

    /**
     * constructor.
     */
    public ImportFileFilter() {
    }

    /**
     * Check acceptance of file.
     *
     * @param file file to test
     * @return true if accepted, false otherwise
     */
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

  /**
   * Extension of class for timing the importing.
   */
  class SchedulerTimerTask extends TimerTask {

    // instance variables
    private ImportFileFilter importFileFilter;
    private File[] selectedFiles;
    private File archiveFile;
    private Importer importer;

    /**
     * constructor.
     */
    public SchedulerTimerTask() {
      importFileFilter = new ImportFileFilter();
    }

    /**
     * run.
     */
    public void run() {

      // check for new files
      selectedFiles = dataDir.listFiles(importFileFilter);

      // sort the array by filename
      Arrays.sort(selectedFiles, new FileComparator());

      // output information related to this scheduler
      LOGGER.info("");
      LOGGER.info("{} begin polling cycle", Time.toDateString(currentTime.now()));
      LOGGER.info("files:{}", selectedFiles.length);

      // if there are new file, then we can instantiate the class, thus creating a db connection
      // only try and make a db connection if we need one, no need having a connection open
      // all day long if we only use it for five seconds a day.
      if (selectedFiles.length > 0) {

        // instantiate the import class
        try {
          importClass = Class.forName(importerName);
        } catch (ClassNotFoundException e) {
          LOGGER.error("importer not found");
          System.exit(-1);
        }

        // this will call the default constructor of the class
        try {
          importer = (Importer) importClass.newInstance();
        } catch (InstantiationException e) {
          LOGGER.error("{} InstantiationException", importerName);
          System.exit(-1);
        } catch (IllegalAccessException e) {
          LOGGER.error("{} IllegalAccessException", importerName);
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
            LOGGER.info("archiving {} to {}", file.getAbsolutePath(), archiveDir.getAbsolutePath());
            archiveFile = new File(archiveDir, file.getName());
            try {
              FileCopy.fileCopy(file, archiveFile);
            } catch (IOException e) {
              LOGGER.error("error copying file to archive directory");
            }
          }

          // rename the file if requested
          if (delete) {
            LOGGER.info("deleting {}", file.getAbsolutePath());
            if (!file.delete()) {
              LOGGER.error("error deleting {}", file.getName());
            }
          }
        }

        // de-initialize the importer
        importer.deinitialize();
      }

      // output some loggin info for this iteration
      LOGGER.info("{} end polling cycle", Time.toDateString(currentTime.now()));
    }
  }

  private static class FileComparator implements Comparator {

    private Collator collator = Collator.getInstance();

    /**
     * Yield "difference" between o1 & o2.
     *
     * @param o1 first object to compare
     * @param o2 second object to compare
     * @return difference, where directories are < files
     */
    public int compare(Object o1, Object o2) {
      if (o1 == o2) {
        return 0;
      }

      File f1 = (File) o1;
      File f2 = (File) o2;

      if (f1.isDirectory() && f2.isFile()) {
        return -1;
      }
      if (f1.isFile() && f2.isDirectory()) {
        return 1;
      }

      return collator.compare(f1.getName(), f2.getName());
    }
  }
}
