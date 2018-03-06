package gov.usgs.volcanoes.vdx.in.rsam;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.legacy.Arguments;
import gov.usgs.volcanoes.vdx.in.ImportBob;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Import BobRSAM Data.
 *
 * @author Tom Parker
 * @author Bill Tollett
 */
public class ImportBobRsam {

  private static final String CONFIG_FILE = "importBobRsam.config";
  private int year;
  public ConfigFile params;

  /**
   * Constructor.
   *
   * @param cf configuration file
   * @param y year
   */
  public ImportBobRsam(String cf, int y) {
    params = new ConfigFile(cf);
    if (!params.wasSuccessfullyRead()) {
      System.out.println(cf + ": could not read config file.");
      System.exit(1);
    }
    year = y;
  }

  /**
   * Import data.
   *
   * @param dirName directory to find files to import
   * @param fileType type (events/values)
   */
  private void processDir(String dirName, String fileType) {
    File dir = null;

    dir = new File(dirName);
    try {
      validateDirectory(dir);
    } catch (FileNotFoundException e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }

    for (File f : dir.listFiles()) {
      process(f, fileType);
    }
  }

  /**
   * Import data from file defined by "valueDir" configuration parameter and events from file
   * defined by "eventDir" configuration parameter.
   */
  public void process() {
    String dirName;

    dirName = params.getString("valueDir");
    if (dirName != null) {
      processDir(dirName, "ewrsamValues");
    }

    dirName = params.getString("eventDir");
    if (dirName != null) {
      processDir(dirName, "ewrsamEvents");
    }
  }

  /**
   * Import data.
   *
   * @param f file to import
   * @param t type (events/values)
   */
  private void process(File f, String t) {
    String channel;
    int fileYear;

    // look for standard bob filename
    Pattern p1 = Pattern.compile("(\\w{4})(\\d{4})\\.DAT$");
    Matcher m1 = p1.matcher(f.getName());

    // look for non-bob-compliant full SCN in filename.
    // allow for non-standard UAF style EHZ4 component
    Pattern p2 = Pattern.compile("(\\w{4}):(\\w{3}4?):(\\w{2})(\\d{4})\\.DAT$");
    Matcher m2 = p2.matcher(f.getName());

    if (m1.matches()) {
      channel = m1.group(1);
      if (channel.matches("\\w{3}_")) {
        channel = channel.substring(0, 3);
      }

      channel = params.getString(channel);
      fileYear = Integer.parseInt(m1.group(2));
    } else if (m2.matches()) {
      channel = m2.group(1) + "$" + m2.group(2) + "$" + m2.group(3);
      fileYear = Integer.parseInt(m2.group(4));
    } else {
      System.out.println("ignoring poorly named file " + f);
      return;
    }

    if ((year != -1) && (fileYear != year)) {
      return;
    }

    System.out.println(
        "importing " + channel + " " + t + " data for the year " + fileYear + " from file " + f
            .getAbsolutePath());
    ImportBob ib = new ImportBob(params.getString("vdxConfig"), fileYear,
        params.getString("prefix"), t);
    ib.process(channel, f.getAbsolutePath());
  }

  /**
   * Check if directory exist and readable.
   *
   * @param direcotry directory to validate
   * @throws FileNotFoundException if not found
   */
  private static void validateDirectory(File direcotry) throws FileNotFoundException {
    if (direcotry == null) {
      throw new IllegalArgumentException("Directory should not be null.");
    } else if (!direcotry.exists()) {
      throw new FileNotFoundException("Directory does not exist: " + direcotry);
    } else if (!direcotry.isDirectory()) {
      throw new IllegalArgumentException("Is not a directory: " + direcotry);
    } else if (!direcotry.canRead()) {
      throw new IllegalArgumentException("Directory cannot be read: " + direcotry);
    }
  }

  /**
   * Main method Syntax is: java gov.usgs.volcanoes.vdx.data.rsam.ImportBobRsam [-c [configFile]]
   * [-Y | -y [year] | -a] Options: -c [configFile] config to use, Default: importBobRsam.config -Y
   * import data for this year -y [year] import data for given year -a import all data
   *
   * @param as command line args
   */
  public static void main(String[] as) {
    Calendar startTime = Calendar.getInstance();
    String cf = CONFIG_FILE;
    int y = -2;

    System.out.println("Starting import at " + startTime.toString());

    Set<String> flags = new HashSet<String>();
    Set<String> kvs = new HashSet<String>();
    kvs.add("-c");
    kvs.add("-y");
    flags.add("-a");
    flags.add("-Y");

    Arguments args = new Arguments(as, flags, kvs);

    if (args.contains("-c")) {
      cf = args.get("-c");
    }

    if (args.contains("-y")) {
      y = Integer.parseInt(args.get("-y"));
    } else if (args.flagged("-Y")) {
      y = startTime.get(Calendar.YEAR);
    } else if (args.flagged("-a")) {
      y = -1;
    }

    if (args.contains("-h") || y == -2) {
      System.err.println(
          "java gov.usgs.volcanoes.vdx.data.rsam.ImportBobRsam [-c <configFile>] "
              + "[-Y | -y <year> | -a]");
      System.err.println("\t-c <configFile>\tconfig to use, Default: importBobRsam.config");
      System.err.println("\t-Y\timport data for this year");
      System.err.println("\t-y <year>\timport data for given year");
      System.err.println("\t-a\timport all data");
      System.exit(-1);
    }

    ImportBobRsam in = new ImportBobRsam(cf, y);
    in.process();

    Calendar endTime = Calendar.getInstance();
    double ellapsedTime = (endTime.getTimeInMillis() - startTime.getTimeInMillis()) / 1000;
    System.out.println("Import finished at " + endTime.toString());
    System.out.println("Elapsed time: " + ellapsedTime + " seconds");
  }
}
