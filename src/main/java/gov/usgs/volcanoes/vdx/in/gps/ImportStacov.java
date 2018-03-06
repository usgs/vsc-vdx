package gov.usgs.volcanoes.vdx.in.gps;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.legacy.Arguments;
import gov.usgs.volcanoes.core.time.J2kSec;
import gov.usgs.volcanoes.core.util.ResourceReader;
import gov.usgs.volcanoes.core.util.StringUtils;
import gov.usgs.volcanoes.vdx.data.Channel;
import gov.usgs.volcanoes.vdx.data.Column;
import gov.usgs.volcanoes.vdx.data.Rank;
import gov.usgs.volcanoes.vdx.data.SQLDataSource;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceDescriptor;
import gov.usgs.volcanoes.vdx.data.SQLDataSourceHandler;
import gov.usgs.volcanoes.vdx.data.gps.Gps;
import gov.usgs.volcanoes.vdx.data.gps.SQLGpsDataSource;
import gov.usgs.volcanoes.vdx.data.gps.SolutionPoint;
import gov.usgs.volcanoes.vdx.in.Importer;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Import Stacov files.
 *
 * @author Dan Cervelli
 * @author Loren Antolik
 * @author Bill Tollett
 */
public class ImportStacov implements Importer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImportStacov.class);

  public ResourceReader rr;

  public static Set<String> flags;
  public static Set<String> keys;

  public String vdxConfig;

  public ConfigFile params;
  public ConfigFile vdxParams;
  public ConfigFile rankParams;
  public ConfigFile columnParams;
  public ConfigFile channelParams;
  public ConfigFile dataSourceParams;
  public ConfigFile translationParams;

  public String driver;
  public String prefix;
  public String url;

  public SimpleDateFormat dateIn;
  public SimpleDateFormat dateOut;
  public Date date;
  public Double j2ksec;

  public String filenameMask;
  public int headerLines;
  public String timeZone;

  public String importColumns;
  public String[] importColumnArray;
  public Map<Integer, String> importColumnMap;

  public String dataSource;
  public SQLGpsDataSource sqlDataSource;
  public SQLDataSourceHandler sqlDataSourceHandler;
  public SQLDataSourceDescriptor sqlDataSourceDescriptor;
  public List<String> dataSourceList;
  public Iterator<String> dsIterator;
  public Map<String, SQLDataSource> sqlDataSourceMap;
  public Map<String, String> dataSourceColumnMap;
  public Map<String, String> dataSourceChannelMap;
  public Map<String, Integer> dataSourceRankMap;

  public Rank rank;
  public String rankName;
  public int rankValue;
  public int rankDefault;
  public int rid;

  public String channels;
  public String[] channelArray;
  public Map<String, Channel> channelMap;
  public Channel channel;
  public String channelCode;
  public String channelName;
  public double channelLon;
  public double channelLat;
  public double channelHeight;
  public List<String> channelList;
  public Iterator<String> chIterator;
  public String defaultChannels;

  public String columns;
  public String[] columnArray;
  public HashMap<String, Column> columnMap;
  public Column column;
  public String columnName;
  public String columnDescription;
  public String columnUnit;
  public int columnIdx;
  public boolean columnActive;
  public boolean columnChecked;
  public List<String> columnList;
  public Iterator<String> coIterator;
  public String defaultColumns;

  public String importerType = "gps";

  static {
    flags = new HashSet<String>();
    keys = new HashSet<String>();
    keys.add("-c");
    flags.add("-h");
    flags.add("-v");
  }

  /**
   * takes a config file as a parameter and parses it to prepare for importing.
   *
   * @param importerClass name of importer class to use
   * @param configFile configuration file
   * @param verbose true for info, false for severe
   */
  public void initialize(String importerClass, String configFile, boolean verbose) {

    // initialize the LOGGER for this importer
    LOGGER.info("ImportStacov.initialize() succeeded.");

    // process the config file
    processConfigFile(configFile);
  }

  /**
   * disconnects from the database.
   */
  public void deinitialize() {
    sqlDataSource.disconnect();
  }

  /**
   * Parse configuration file.  This sets class variables used in the importing process
   *
   * @param configFile name of the config file
   */
  public void processConfigFile(String configFile) {

    LOGGER.info("Reading config file {}", configFile);

    // initialize the config file and verify that it was read
    params = new ConfigFile(configFile);
    if (!params.wasSuccessfullyRead()) {
      LOGGER.error("{} was not successfully read", configFile);
      System.exit(-1);
    }

    // get the vdx parameter, and exit if it's missing
    vdxConfig = StringUtils.stringToString(params.getString("vdx.config"), "VDX.config");
    if (vdxConfig == null) {
      LOGGER.error("vdx.config parameter missing from config file");
      System.exit(-1);
    }

    // get the vdx config as it's own config file object
    vdxParams = new ConfigFile(vdxConfig);
    driver = vdxParams.getString("vdx.driver");
    url = vdxParams.getString("vdx.url");
    prefix = vdxParams.getString("vdx.prefix");

    // define the data source handler that acts as a wrapper for data sources
    sqlDataSourceHandler = new SQLDataSourceHandler(driver, url, prefix);

    // get the list of data sources that are being used in this import
    dataSource = params.getString("dataSource");

    // lookup the data source from the list that is in vdxSources.config
    sqlDataSourceDescriptor = sqlDataSourceHandler.getDataSourceDescriptor(dataSource);
    if (sqlDataSourceDescriptor == null) {
      LOGGER.error("{} sql data source does not exist in vdxSources.config", dataSource);
    }

    // formally get the data source from the list of descriptors.
    // this will initialize the data source which includes db creation
    sqlDataSource = (SQLGpsDataSource) sqlDataSourceDescriptor.getSQLDataSource();

    if (!sqlDataSource.getType().equals(importerType)) {
      LOGGER.error("dataSource not a {} data source", importerType);
      System.exit(-1);
    }

    // information related to the timestamps
    dateIn = new SimpleDateFormat("yyMMMdd");
    dateIn.setTimeZone(TimeZone.getTimeZone("GMT"));

    // get the list of ranks that are being used in this import
    rankParams = params.getSubConfig("rank");
    rankName = StringUtils.stringToString(rankParams.getString("name"), "Raw Data");
    rankValue = StringUtils.stringToInt(rankParams.getString("value"), 1);
    rankDefault = StringUtils.stringToInt(rankParams.getString("default"), 0);
    rank = new Rank(0, rankName, rankValue, rankDefault);

    // create rank entry
    if (sqlDataSource.getRanksFlag()) {
      Rank tempRank = sqlDataSource.defaultGetRank(rank);
      if (tempRank == null) {
        tempRank = sqlDataSource.defaultInsertRank(rank);
      }
      if (tempRank == null) {
        LOGGER.error("invalid rank for dataSource {}", dataSource);
        System.exit(-1);
      }
      rid = tempRank.getId();
    }

    // get the list of channels and create a hash map keyed with the channel code
    List<Channel> chs = sqlDataSource.getChannelsList();
    channelMap = new HashMap<String, Channel>();
    for (Channel ch : chs) {
      channelMap.put(ch.getCode(), ch);
    }
  }

  /**
   * Parse stacov file from url (resource locator or filename).
   */
  public void process(String filename) {

    // initialize variables local to this method
    String sx;
    String sy;
    String sz;
    String sc;
    int p1;
    int p2;
    int i1;
    int i2;
    double data;
    double[] llh;
    SolutionPoint sp;
    Channel channel;
    boolean done;

    try {

      // check that the file exists
      rr = ResourceReader.getResourceReader(filename);
      if (rr == null) {
        LOGGER.error("skipping: {} (resource is invalid)", filename);
        return;
      }

      // move to the first line in the file
      String line = rr.nextLine();

      // check that the file has data
      if (line == null) {
        LOGGER.error("skipping: {} (resource is empty)", filename);
        return;
      }

      // read the first line and get soltion count information
      int numParams = Integer.parseInt(line.substring(0, 5).trim());
      SolutionPoint[] points = new SolutionPoint[numParams / 3];

      // read the first line and get date information
      double j2ksec0;
      double j2ksec1;
      try {
        String timestamp = line.substring(20, 27);
        date = dateIn.parse(timestamp);
        date.setTime(date.getTime());
        j2ksec0 = J2kSec.fromDate(date);
        j2ksec1 = j2ksec0 + 86400;
      } catch (ParseException e) {
        LOGGER.error("skipping: {}  (timestamp not valid)", filename);
        return;
      }

      // Get md5 resource
      String md5 = null;
      try {
        MessageDigest md = MessageDigest.getInstance("MD5");
        if (md != null) {
          InputStream in = null;
          if (filename.indexOf("://") != -1) {
            URL url = new URL(filename);
            in = url.openStream();
          } else {
            in = new FileInputStream(new File(filename));
          }

          int nr = 0;
          byte[] buf = new byte[64 * 1024];
          while (nr != -1) {
            nr = in.read(buf);
            if (nr != -1) {
              md.update(buf, 0, nr);
            }
          }
          in.close();

          ByteBuffer bb = ByteBuffer.wrap(md.digest());
          StringBuffer sb = new StringBuffer(32);
          for (int i = 0; i < 4; i++) {
            String h = Integer.toHexString(bb.getInt());
            for (int j = h.length(); j < 8; j++) {
              sb.append('0');
            }
            sb.append(h);
          }
          md5 = sb.toString();
        }
      } catch (Exception e) {
        LOGGER.info("Problem getting the md5 of the resource {}", filename);
      }

      // attempt to insert this source.
      // this method will tell if this file has already been imported
      int sid = sqlDataSource
          .insertSource(new File(filename).getName(), md5, j2ksec0, j2ksec1, rid);
      if (sid == -1) {
        LOGGER.error("skipping: {} (hash already exists)", filename);
        return;
      }

      LOGGER.info("importing: {}", filename);

      for (int i = 0; i < numParams / 3; i++) {
        sx = rr.nextLine();
        sy = rr.nextLine();
        sz = rr.nextLine();
        sp = new SolutionPoint();

        sp.channel = sx.substring(7, 11).trim();
        sp.dp.timeVal = (j2ksec0 + j2ksec1) / 2;
        sp.dp.xcoord = Double.parseDouble(sx.substring(25, 47).trim());
        sp.dp.sxx = Double.parseDouble(sx.substring(53, 74).trim());

        sp.dp.ycoord = Double.parseDouble(sy.substring(25, 47).trim());
        sp.dp.syy = Double.parseDouble(sy.substring(53, 74).trim());

        sp.dp.zcoord = Double.parseDouble(sz.substring(25, 47).trim());
        sp.dp.szz = Double.parseDouble(sz.substring(53, 74).trim());

        points[i] = sp;
      }

      done = false;
      while (!done) {
        try {
          sc = rr.nextLine();
          if (sc != null && sc.length() >= 2) {
            p1 = Integer.parseInt(sc.substring(0, 5).trim()) - 1;
            p2 = Integer.parseInt(sc.substring(5, 11).trim()) - 1;
            data = Double.parseDouble(sc.substring(13).trim());
            if (p1 / 3 == p2 / 3) {
              sp = points[p1 / 3];
              i1 = Math.min(p1 % 3, p2 % 3);
              i2 = Math.max(p1 % 3, p2 % 3);
              if (i1 == 0 && i2 == 1) {
                sp.dp.sxy = data;
              } else if (i1 == 0 && i2 == 2) {
                sp.dp.sxz = data;
              } else if (i1 == 1 && i2 == 2) {
                sp.dp.syz = data;
              }
            }
          } else {
            done = true;
          }
        } catch (NumberFormatException e) {
          done = true;
        }
      }
      rr.close();
      for (SolutionPoint spt : points) {
        spt.dp.sxy = spt.dp.sxy * spt.dp.sxx * spt.dp.syy;
        spt.dp.sxz = spt.dp.sxz * spt.dp.sxx * spt.dp.szz;
        spt.dp.syz = spt.dp.syz * spt.dp.syy * spt.dp.szz;
        spt.dp.sxx = spt.dp.sxx * spt.dp.sxx;
        spt.dp.syy = spt.dp.syy * spt.dp.syy;
        spt.dp.szz = spt.dp.szz * spt.dp.szz;

        channel = channelMap.get(spt.channel);

        // if the channel isn't in the channel list from the db then it needs to be created
        if (channel == null) {
          llh = Gps.xyz2llh(spt.dp.xcoord, spt.dp.ycoord, spt.dp.zcoord);
          sqlDataSource.createChannel(spt.channel, spt.channel, llh[0], llh[1], llh[2], 1);
          channel = sqlDataSource.getChannel(spt.channel);
          channelMap.put(spt.channel, channel);
        }

        // insert the solution into the db
        sqlDataSource.insertSolution(sid, channel.getCId(), spt.dp);
      }

    } catch (Exception e) {
      LOGGER.error("ImportStacov.process({}) failed.", filename, e);
    }
  }

  /**
   * Output instructions.
   * @param importerClass name of importer class
   * @param message instructions
   */
  public void outputInstructions(String importerClass, String message) {
    if (message == null) {
      System.err.println(message);
    }
    System.err.println(importerClass + " -c configfile filelist");
  }

  /**
   * Main method. Command line syntax: -h, --help print help message -c config file name -v verbose
   * mode files ...
   *
   * @param as command line args
   */
  public static void main(String[] as) {

    ImportStacov importer = new ImportStacov();

    Arguments args = new Arguments(as, flags, keys);

    if (args.flagged("-h")) {
      importer.outputInstructions(importer.getClass().getName(), null);
      System.exit(-1);
    }

    if (!args.contains("-c")) {
      importer.outputInstructions(importer.getClass().getName(), "Config file required");
      System.exit(-1);
    }

    importer.initialize(importer.getClass().getName(), args.get("-c"), args.flagged("-v"));

    List<String> files = args.unused();
    for (String file : files) {
      importer.process(file);
    }

    importer.deinitialize();
  }
}
