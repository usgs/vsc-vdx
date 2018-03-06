package gov.usgs.volcanoes.vdx.in;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.legacy.Arguments;
import gov.usgs.volcanoes.core.util.ResourceReader;
import gov.usgs.volcanoes.vdx.data.Channel;
import gov.usgs.volcanoes.vdx.data.Column;
import gov.usgs.volcanoes.vdx.data.DataSourceDescriptor;
import gov.usgs.volcanoes.vdx.data.DataSourceHandler;
import gov.usgs.volcanoes.vdx.data.SQLDataSource;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceHandler;
import gov.usgs.volcanoes.vdx.data.SuppDatum;
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

public class ImportSuppdata {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImportSuppdata.class);
  private static Set<String> flags;
  private static Set<String> keys;
  private Map<String, Integer> channelMap;
  private Map<String, Integer> columnMap;
  private Map<String, Integer> rankMap;
  private Map<String, Integer> sdtypeMap;
  private VDXDatabase database;
  private ConfigFile vdxParams;
  private DataSourceHandler dataSourceHandler;
  private SQLDataSourceHandler sqlDataSourceHandler;

  static {
    flags = new HashSet<String>();
    keys = new HashSet<String>();
    keys.add("-c");
    flags.add("-h");
    flags.add("-cm");
    flags.add("-v");
  }

  /**
   * Initialize importer.
   */
  public void initialize(String importerClass, String configFile) {

    // initialize the LOGGER for this importer
    LOGGER.info("ImportSuppdata.initialize() succeeded.");

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
    vdxParams = new ConfigFile(vdxConfig);
    String driver = vdxParams.getString("vdx.driver");
    String url = vdxParams.getString("vdx.url");
    String prefix = vdxParams.getString("vdx.prefix");
    database = new VDXDatabase(driver, url, prefix);
    dataSourceHandler = new DataSourceHandler(driver, url, prefix);
    sqlDataSourceHandler = new SQLDataSourceHandler(driver, url, prefix);
  }

  /**
   * Process.  Reads a file and parses the contents to the database
   */
  public void process(String pathname) {
    File file = new File(pathname);
    String filename = file.getName();
    String source = filename.substring(5, filename.length() - 19);
    SQLDataSourceDescriptor dsd = sqlDataSourceHandler.getDataSourceDescriptor(source);
    if (dsd == null) {
      LOGGER.error("skipping: {} (datasource is invalid)", pathname);
      return;
    }
    SQLDataSource ds = null;
    VDXSource vds = null;
    try {
      Object ods = dsd.getSQLDataSource();
      if (ods != null) {
        ds = dsd.getSQLDataSource();
      }
    } catch (Exception e) {
      LOGGER.error("Problem getting datasource");
    }

    int defaultRank = 0;
    int lineLen1;
    int lineLen2;
    if (ds != null) {
      // This is an SQL DataSource

      // Build map of channels
      channelMap = new HashMap<String, Integer>();
      for (Channel ch : ds.defaultGetChannelsList(false)) {
        channelMap.put(ch.getCode(), ch.getCId());
      }
      LOGGER.info("Channels mapped: " + channelMap.size());

      // Build map of columns
      columnMap = new HashMap<String, Integer>();
      for (Column col : ds.defaultGetColumns(true, ds.getMenuColumnsFlag())) {
        columnMap.put(col.name, col.idx);
      }
      LOGGER.info("Columns mapped: " + columnMap.size());

      // Build map of ranks
      rankMap = new HashMap<String, Integer>();
      for (String rk : ds.defaultGetRanks()) {
        String[] rkBits = rk.split(":");
        int id = Integer.parseInt(rkBits[0]);
        rankMap.put(rkBits[1], id);
        if (rkBits[3].equals("1")) {
          defaultRank = id;
        }
      }
      LOGGER.info("Ranks mapped: " + rankMap.size());

      // Set limits on # args per input line
      lineLen1 = 7;
      lineLen2 = 9;

      // Build map of supp data types
      sdtypeMap = new HashMap<String, Integer>();
      for (SuppDatum sdt : ds.getSuppDataTypes()) {
        sdtypeMap.put(sdt.typeName, sdt.tid);
      }
      LOGGER.info("Suppdata types mapped: " + sdtypeMap.size());

    } else {
      // It isn't a SQL datasource; try it as a Winston datasource
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

      // Build map of channels
      channelMap = new HashMap<String, Integer>();
      for (gov.usgs.volcanoes.winston.Channel ch : vds.getChannels().getChannels()) {
        channelMap.put(ch.toString(), ch.sid);
      }
      LOGGER.info("Channels mapped: {}", channelMap.size());

      // Set limits on # args per input line
      lineLen1 = lineLen2 = 7;

      // Build map of supp data types
      sdtypeMap = new HashMap<String, Integer>();
      try {
        for (SuppDatum sdt : vds.getSuppDataTypes()) {
          sdtypeMap.put(sdt.typeName, sdt.tid);
        }
        LOGGER.info("Suppdata types mapped: " + sdtypeMap.size());
      } catch (Exception e3) {
        LOGGER.error(
            "skipping: " + pathname + " (problem reading supplemental data types)");
        return;
      }
    }

    // Access the input file
    ResourceReader rr = ResourceReader.getResourceReader(pathname);
    if (rr == null) {
      LOGGER.error("skipping: {} (resource is invalid)", pathname);
      return;
    }
    // move to the first line in the file
    String line = rr.nextLine();
    int lineNumber = 0;

    // check that the file has data
    if (line == null) {
      LOGGER.error("skipping: {} (resource is empty)", pathname);
      return;
    }

    LOGGER.info("importing: " + filename);

    SuppDatum sd = new SuppDatum();
    int success = 0;

    // we are now at the first row of data.  time to import!
    String[] valueArray = new String[lineLen2];
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
      boolean ok = true;
      for (int j = 0; ok && j < quoteParts.length; j += 2) {
        String[] parts = quoteParts[j].split(",", -1);
        int k1 = 1;
        int k2 = parts.length - 1;
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
            LOGGER.warn("Aborting import of line {}, trailing comma", lineNumber);
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
      if (!ok) {
        continue;
      }

      // Validate & unmap arguments
      if (valueArrayLength < lineLen1) {
        LOGGER.warn("Aborting import of line {}, too few elements ({})",
            lineNumber, valueArrayLength);
        continue;
      }
      try {
        sd.cid = channelMap.get(valueArray[3].trim());
      } catch (Exception e) {
        LOGGER.warn("Aborting import of line {}, unknown channel: '{}'", lineNumber, valueArray[3]);
        continue;
      }
      try {
        sd.st = Double.parseDouble(valueArray[1].trim());
      } catch (Exception e) {
        LOGGER.warn("Aborting import of line {}, invalid start time: '{}'",
            lineNumber, valueArray[1]);
        continue;
      }
      try {
        String et = valueArray[2].trim();
        if (et.length() == 0) {
          sd.et = Double.MAX_VALUE;
        } else {
          sd.et = Double.parseDouble(et);
        }
      } catch (Exception e) {
        LOGGER.warn("Aborting import of line {}, invalid end time: '{}'",
            lineNumber, valueArray[2]);
        continue;
      }
      try {
        sd.typeName = valueArray[4].trim();
        Integer tid = sdtypeMap.get(sd.typeName);
        if (tid == null) {
          sd.color = "000000";
          sd.dl = 1;
          sd.tid = -1;
        } else {
          sd.tid = tid;
        }
      } catch (Exception e) {
        LOGGER.warn("Aborting import of line {}, couldn't create type: '{}'",
            lineNumber, valueArray[4]);
        continue;
      }
      if (ds != null) {
        if (valueArrayLength > lineLen1) {
          try {
            sd.colid = columnMap.get(valueArray[7].trim());
          } catch (Exception e) {
            LOGGER.warn("Aborting import of line {}, unknown column: '{}'",
                lineNumber, valueArray[7]);
            continue;
          }
          if (valueArrayLength < lineLen2) {
            sd.rid = defaultRank;
          } else {
            try {
              sd.rid = rankMap.get(valueArray[8].trim());
            } catch (Exception e) {
              LOGGER.warn("Aborting import of line {}, unknown rank: '{}'",
                  lineNumber, valueArray[8]);
              continue;
            }
          }
        } else {
          sd.colid = -1;
          sd.rid = -1;
        }
      } else {
        sd.colid = -1;
        sd.rid = -1;
      }
      sd.name = valueArray[5].trim();
      sd.value = valueArray[6].trim();

      try {
        sd.sdid = Integer.parseInt(valueArray[0]);
      } catch (Exception e) {
        LOGGER.warn("Aborting import of line {}, unknown id: '{}'", lineNumber, valueArray[0]);
        continue;
      }

      // Finally, insert/update the data
      try {
        if (ds != null) {
          if (sd.tid == -1) {
            sd.tid = ds.insertSuppDataType(sd);
            if (sd.tid == 0) {
              LOGGER.warn("Aborting import of line {}, problem inserting datatype", lineNumber);
              continue;
            }
            sdtypeMap.put(sd.typeName, sd.tid);
            LOGGER.info("Added supplemental datatype {}", sd.typeName);
          }
          int readSdid = sd.sdid;
          if (sd.sdid == 0) {
            sd.sdid = ds.insertSuppDatum(sd);
          } else {
            sd.sdid = ds.updateSuppDatum(sd);
          }
          if (sd.sdid < 0) {
            sd.sdid = -sd.sdid;
            LOGGER.info("For import of line {}, supp data record already exists as SDID {}; "
                + "will create xref record", lineNumber, sd.sdid);
          } else if (sd.sdid == 0) {
            LOGGER.warn("Aborting import of line {}, problem {}ing supp data",
                lineNumber, (readSdid == 0 ? "insert" : "updat"));
            continue;
          } else if (readSdid == 0) {
            LOGGER.info("Added supp data record SDID {}", sd.sdid);
          } else {
            LOGGER.info("Updated supp data record SDID {}", sd.sdid);
          }
          if (!ds.insertSuppDatumXref(sd)) {
            continue;
          } else {
            LOGGER.info("Added xref for SDID {}", sd.sdid);
          }
        } else {
          if (sd.tid == -1) {
            sd.tid = vds.insertSuppDataType(sd);
            if (sd.tid == 0) {
              LOGGER.warn("Aborting import of line {}, problem inserting datatype", lineNumber);
              continue;
            }
            sdtypeMap.put(sd.typeName, sd.tid);
            LOGGER.info("Added supplemental datatype {}", sd.typeName);
          }
          int readSdid = sd.sdid;
          if (sd.sdid == 0) {
            sd.sdid = vds.insertSuppDatum(sd);
          } else {
            sd.sdid = vds.updateSuppDatum(sd);
          }
          if (sd.sdid < 0) {
            sd.sdid = -sd.sdid;
            LOGGER.info("For import of line {}, supp data record already exists as SDID {}; "
                + "will create xref record", lineNumber, sd.sdid);
          } else if (sd.sdid == 0) {
            LOGGER.warn("Aborting import of line {}, problem {}ing supp data",
                lineNumber, (readSdid == 0 ? "insert" : "updat"));
            continue;
          } else if (readSdid == 0) {
            LOGGER.info("Added supp data record SDID {}", sd.sdid);
          } else {
            LOGGER.info("Updated supp data record SDID {}", sd.sdid);
          }
          if (!vds.insertSuppDatumXref(sd)) {
            continue;
          } else {
            LOGGER.info("Added xref for SDID {}", sd.sdid);
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Failed import of line {}, db failure: ", lineNumber, e);
        continue;
      }
      success++;
    }
    LOGGER.info("{} of {} lines successfully processed", success, lineNumber);
  }

  /**
   * Print usage.  Prints out usage instructions for the given importer
   */
  public void outputInstructions(String importerClass, String message) {
  }

  /**
   * Main method. Command line syntax: -h, --help print help message -c config file name files ...
   */
  public static void main(String[] as) {
    ImportSuppdata importer = new ImportSuppdata();

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
