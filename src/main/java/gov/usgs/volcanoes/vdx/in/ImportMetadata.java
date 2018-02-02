package gov.usgs.volcanoes.vdx.in;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.legacy.Arguments;
import gov.usgs.volcanoes.core.util.ResourceReader;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Import metadata files.
 *
 * @author Scott B. Hunter, ISTI
 * @author Bill Tollett
 */
public class ImportMetadata {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImportMetadata.class);
  private static Set<String> flags;
  private static Set<String> keys;
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

    LOGGER.info("ImportMetadata.initialize() succeeded.");

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
      LOGGER.error("skipping: {} (datasource is invalid)", pathname);
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
      LOGGER.error("Problem getting datasource");
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
      LOGGER.info("Channels mapped: {}", channelMap.size());

      columnMap = new HashMap<String, Integer>();
      for (Column col : ds.defaultGetColumns(true, ds.getMenuColumnsFlag())) {
        columnMap.put(col.name, col.idx);
      }
      LOGGER.info("Columns mapped: {}", columnMap.size());

      rankMap = new HashMap<String, Integer>();
      for (String rk : ds.defaultGetRanks()) {
        String[] rkBits = rk.split(":");
        int id          = Integer.parseInt(rkBits[0]);
        rankMap.put(rkBits[1], id);
        if (rkBits[3].equals("1")) {
          defaultRank = id;
        }
      }
      LOGGER.info("Ranks mapped: {}", rankMap.size());
      lineLen1 = 5;
      lineLen2 = 6;
    } else {
      DataSourceDescriptor vdsd = dataSourceHandler.getDataSourceDescriptor(source);
      try {
        vds = (VDXSource) vdsd.getDataSource();
        if (vds == null) {
          LOGGER.error("skipping: {} (datasource is invalid)", pathname);
          return;
        }
      } catch (Exception e2) {
        LOGGER.error("skipping: {} (datasource is invalid)", pathname);
        return;
      }
      channelMap = new HashMap<String, Integer>();
      for (gov.usgs.volcanoes.winston.Channel ch : vds.getChannels().getChannels()) {
        channelMap.put(ch.toString(), ch.sid);
      }
      LOGGER.info("Channels mapped: " + channelMap.size());

      lineLen1 = lineLen2 = 4;
    }

    ResourceReader rr = ResourceReader.getResourceReader(pathname);
    if (rr == null) {
      LOGGER.error("skipping: {} (resource is invalid)", pathname);
      return;
    }
    // move to the first line in the file
    String line    = rr.nextLine();
    int lineNumber = 0;

    // check that the file has data
    if (line == null) {
      LOGGER.error("skipping: {} (resource is empty)", pathname);
      return;
    }

    LOGGER.info("importing: " + filename);

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
        LOGGER.warn("Aborting import of line {}, mismatched quotes", lineNumber);
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
            LOGGER.warn("Aborting import of line {}, leading comma", lineNumber);
            ok = false;
            break;
          }
          k1--;
        }
        if (j == quoteParts.length - 1) { // section after last quote
          middle = false;
          if (parts.length > 1 && parts[parts.length - 1].trim().length() == 0) {
            LOGGER.warn("Aborting import of line " + lineNumber + ", trailing comma", lineNumber);
            ok = false;
            break;
          }
          k2++;
        }
        if (middle) {
          if (parts.length == 1) {
            LOGGER.warn("Aborting import of line {}, missing comma between quotes", lineNumber);
            ok = false;
            break;
          }
          if (parts[0].trim().length() != 0) {
            LOGGER.warn("Aborting import of line {}, missing comma after a quote", lineNumber);
            ok = false;
            break;
          }
          if (parts[parts.length - 1].trim().length() != 0) {
            LOGGER.warn("Aborting import of line {}, missing comma before a quote", lineNumber);
            ok = false;
            break;
          }
        }
        int k;
        for (k = k1; ok && k < k2; k++) {
          if (valueArrayLength == lineLen2) {
            LOGGER.warn("Aborting import of line {}, too many elements", lineNumber);
            ok = false;
            break;
          }
          valueArray[valueArrayLength++] = parts[k];
        }
        if (j + 1 < quoteParts.length) {
          if (valueArrayLength == lineLen2) {
            LOGGER.warn("Aborting import of line {}, too many elements", lineNumber);
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
        LOGGER.warn("Aborting import of line {}, wrong number of elements", lineNumber);
        continue;
      }
      try {
        md.cid = channelMap.get(valueArray[1].trim());
      } catch (Exception e) {
        LOGGER.warn("Aborting import of line {}, unknown channel: {}", lineNumber, valueArray[1]);
        continue;
      }
      if (ds != null) {
        try {
          md.colid = columnMap.get(valueArray[4].trim());
        } catch (Exception e) {
          LOGGER.warn("Aborting import of line {}, unknown column: {}", lineNumber, valueArray[4]);
          continue;
        }
        if (valueArrayLength == lineLen1) {
          md.rid = defaultRank;
        } else {
          try {
            md.rid = rankMap.get(valueArray[5].trim());
          } catch (Exception e) {
            LOGGER.warn("Aborting import of line {}, unknown rank: {}", lineNumber, valueArray[5]);
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
        LOGGER.warn("Aborting import of line {}, unknown id: {}", lineNumber, valueArray[0]);
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
        LOGGER.warn("Failed import of line {}, db failure: ", lineNumber, e);
        continue;
      }
      success++;
    }
    LOGGER.info("" + success + " of " + lineNumber + " lines successfully processed");
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
