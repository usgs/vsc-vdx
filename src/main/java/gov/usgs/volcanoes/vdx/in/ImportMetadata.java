package gov.usgs.volcanoes.vdx.in;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.ResourceReader;
import gov.usgs.volcanoes.vdx.data.Channel;
import gov.usgs.volcanoes.vdx.data.Column;
import gov.usgs.volcanoes.vdx.data.DataSourceDescriptor;
import gov.usgs.volcanoes.vdx.data.DataSourceHandler;
import gov.usgs.volcanoes.vdx.data.MetaDatum;
import gov.usgs.volcanoes.vdx.data.SQLDataSource;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceHandler;
import gov.usgs.volcanoes.vdx.data.VDXSource;
import gov.usgs.volcanoes.vdx.db.VDXDatabase;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Import metadata files.
 *
 * @author Scott B. Hunter, ISTI
 * @author Bill Tollett
 */
public class ImportMetadata {

  private static Set<String> flags;
  private static Set<String> keys;
  private Logger logger;
  private Map<String, Integer> channelMap;
  private Map<String, Integer> columnMap;
  private Map<String, Integer> rankMap;
  private VDXDatabase database;
  private ConfigFile vdxParams;
  private DataSourceHandler dataSourceHandler;
  private SQLDataSourceHandler sqlDataSourceHandler;

  static {
    flags = new HashSet<String>();
    keys  = new HashSet<String>();
    keys.add("-c");
    flags.add("-h");
  }

  /**
   * Initialize importer.
   */
  public void initialize(String importerClass, String configFile) {

    // initialize the logger for this importer
    logger = Logger.getLogger(importerClass);
    logger.info("ImportMetadata.initialize() succeeded.");

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
    vdxParams            = new ConfigFile(vdxConfig);
    String driver        = vdxParams.getString("vdx.driver");
    String url           = vdxParams.getString("vdx.url");
    String prefix        = vdxParams.getString("vdx.prefix");
    database             = new VDXDatabase(driver, url, prefix);
    dataSourceHandler    = new DataSourceHandler(driver, url, prefix);
    sqlDataSourceHandler = new SQLDataSourceHandler(driver, url, prefix);
  }

  /**
   * Process.  Reads a file and parses the contents to the database
   */
  public void process(String pathname) {
    File file = new File(pathname);

    // Isolate filename; use to determine datasource
    String filename = file.getName();
    String source   = filename.substring(5, filename.length() - 19);

    SQLDataSourceDescriptor dsd = sqlDataSourceHandler.getDataSourceDescriptor(source);
    if (dsd == null) {
      logger.log(Level.SEVERE, "skipping: " + pathname + " (datasource is invalid)");
      return;
    }
    SQLDataSource ds = null;
    VDXSource vds    = null;
    try {
      Object ods = dsd.getSQLDataSource();
      if (ods != null) {
        ds = dsd.getSQLDataSource();
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Problem getting datasource");
    }

    // Build Channel and ( if appropriate) column & rank maps
    int defaultRank = 0;
    int lineLen1;
    int lineLen2; // min & max # of args on each input line

    if (ds != null) {
      channelMap = new HashMap<String, Integer>();
      for (Channel ch : ds.defaultGetChannelsList(false)) {
        channelMap.put(ch.getCode(), ch.getCId());
      }
      logger.info("Channels mapped: " + channelMap.size());

      columnMap = new HashMap<String, Integer>();
      for (Column col : ds.defaultGetColumns(true, ds.getMenuColumnsFlag())) {
        columnMap.put(col.name, col.idx);
      }
      logger.info("Columns mapped: " + columnMap.size());

      rankMap = new HashMap<String, Integer>();
      for (String rk : ds.defaultGetRanks()) {
        String[] rkBits = rk.split(":");
        int id          = Integer.parseInt(rkBits[0]);
        rankMap.put(rkBits[1], id);
        if (rkBits[3].equals("1")) {
          defaultRank = id;
        }
      }
      logger.info("Ranks mapped: " + rankMap.size());
      lineLen1 = 5;
      lineLen2 = 6;
    } else {
      DataSourceDescriptor vdsd = dataSourceHandler.getDataSourceDescriptor(source);
      try {
        vds = (VDXSource) vdsd.getDataSource();
        if (vds == null) {
          logger.log(Level.SEVERE, "skipping: " + pathname + " (datasource is invalid)");
          return;
        }
      } catch (Exception e2) {
        logger.log(Level.SEVERE, "skipping: " + pathname + " (datasource is invalid)");
        return;
      }
      channelMap = new HashMap<String, Integer>();
      for (gov.usgs.volcanoes.winston.Channel ch : vds.getChannels().getChannels()) {
        channelMap.put(ch.toString(), ch.sid);
      }
      logger.info("Channels mapped: " + channelMap.size());

      lineLen1 = lineLen2 = 4;
    }

    ResourceReader rr = ResourceReader.getResourceReader(pathname);
    if (rr == null) {
      logger.log(Level.SEVERE, "skipping: " + pathname + " (resource is invalid)");
      return;
    }
    // move to the first line in the file
    String line    = rr.nextLine();
    int lineNumber = 0;

    // check that the file has data
    if (line == null) {
      logger.log(Level.SEVERE, "skipping: " + pathname + " (resource is empty)");
      return;
    }

    logger.info("importing: " + filename);

    MetaDatum md        = new MetaDatum();
    int success         = 0;
    String[] valueArray = new String[lineLen2];

    // we are now at the first row of data.  time to import!
    while (line != null) {
      lineNumber++;
      // Build up array of values in this line
      // First, we split it by quotes
      String[] quoteParts = line.split("'", -1);
      if (quoteParts.length % 2 != 1) {
        logger.warning("Aborting import of line " + lineNumber + ", mismatched quotes");
        continue;
      }
      // Next, walk through those parts, splitting those outside of matching quotes by comma
      int valueArrayLength = 0;
      boolean ok           = true;
      for (int j = 0; ok && j < quoteParts.length; j += 2) {
        String[] parts = quoteParts[j].split(",", -1);
        int k1         = 1;
        int k2         = parts.length - 1;
        boolean middle = true;
        if (j == 0) { // section before first quote
          middle = false;
          if (parts.length > 1 && parts[0].trim().length() == 0) {
            logger.warning("Aborting import of line " + lineNumber + ", leading comma");
            ok = false;
            break;
          }
          k1--;
        }
        if (j == quoteParts.length - 1) { // section after last quote
          middle = false;
          if (parts.length > 1 && parts[parts.length - 1].trim().length() == 0) {
            logger.warning("Aborting import of line " + lineNumber + ", trailing comma");
            ok = false;
            break;
          }
          k2++;
        }
        if (middle) {
          if (parts.length == 1) {
            logger.warning(
                "Aborting import of line " + lineNumber + ", missing comma between quotes");
            ok = false;
            break;
          }
          if (parts[0].trim().length() != 0) {
            logger
                .warning("Aborting import of line " + lineNumber + ", missing comma after a quote");
            ok = false;
            break;
          }
          if (parts[parts.length - 1].trim().length() != 0) {
            logger.warning(
                "Aborting import of line " + lineNumber + ", missing comma before a quote");
            ok = false;
            break;
          }
        }
        int k;
        for (k = k1; ok && k < k2; k++) {
          if (valueArrayLength == lineLen2) {
            logger.warning("Aborting import of line " + lineNumber + ", too many elements");
            ok = false;
            break;
          }
          valueArray[valueArrayLength++] = parts[k];
        }
        if (j + 1 < quoteParts.length) {
          if (valueArrayLength == lineLen2) {
            logger.warning("Aborting import of line " + lineNumber + ", too many elements");
            ok = false;
            break;
          }
          valueArray[valueArrayLength++] = quoteParts[j + 1];
        }
      }

      // Line has been parsed; get next one
      line = rr.nextLine();

      // Validate & unmap arguments
      if (!(valueArrayLength == lineLen1 || valueArrayLength == lineLen2)) {
        logger.warning("Aborting import of line " + lineNumber + ", wrong number of elements");
        continue;
      }
      try {
        md.cid = channelMap.get(valueArray[1].trim());
      } catch (Exception e) {
        logger.warning(
            "Aborting import of line " + lineNumber + ", unknown channel: " + valueArray[1]);
        continue;
      }
      if (ds != null) {
        try {
          md.colid = columnMap.get(valueArray[4].trim());
        } catch (Exception e) {
          logger.warning(
              "Aborting import of line " + lineNumber + ", unknown column: " + valueArray[4]);
          continue;
        }
        if (valueArrayLength == lineLen1) {
          md.rid = defaultRank;
        } else {
          try {
            md.rid = rankMap.get(valueArray[5].trim());
          } catch (Exception e) {
            logger.warning(
                "Aborting import of line " + lineNumber + ", unknown rank: " + valueArray[5]);
            continue;
          }
        }
      } else {
        md.colid = -1;
        md.rid   = -1;
      }
      md.name  = valueArray[2].trim();
      md.value = valueArray[3].trim();
      try {
        md.cmid = Integer.parseInt(valueArray[0]);
      } catch (Exception e) {
        logger.warning("Aborting import of line " + lineNumber + ", unknown id: " + valueArray[0]);
        continue;
      }

      // Finally, insert/update the data
      try {
        if (ds != null) {
          if (md.cmid == 0) {
            ds.insertMetaDatum(md);
          } else {
            ds.updateMetaDatum(md);
          }
        } else if (md.cmid == 0) {
          vds.insertMetaDatum(md);
        } else {
          vds.updateMetaDatum(md);
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
    if (message == null) {
      System.err.println(message);
    }
    System.err.println(importerClass + " [-c configfile] filelist");
  }

  /**
   * Main method. Command line syntax: -h, --help print help message -c config file name files ...
   */
  public static void main(String[] as) {
    ImportMetadata importer = new ImportMetadata();

    Arguments args = new Arguments(as, flags, keys);

    if (args.flagged("-h")) {
      importer.outputInstructions(importer.getClass().getName(), null);
      System.exit(-1);
    }

    String configFile = "VDX.config";
    if (args.contains("-c")) {
      configFile = args.get("-c");
    }

    importer.initialize(importer.getClass().getName(), configFile);
    List<String> files = args.unused();
    for (String file : files) {
      importer.process(file);
    }

    importer.deinitialize();

  }

}
