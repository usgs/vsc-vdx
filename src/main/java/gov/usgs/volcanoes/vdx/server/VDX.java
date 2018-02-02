package gov.usgs.volcanoes.vdx.server;

import gov.usgs.volcanoes.core.Log;
import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.legacy.net.Server;
import gov.usgs.volcanoes.core.util.StringUtils;
import gov.usgs.volcanoes.vdx.Version;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for VDX server.
 *
 * @author Dan Cervelli
 */
public class VDX extends Server {

  private static final Logger LOGGER = LoggerFactory.getLogger(VDX.class);
  protected String configFilename = "VDX.config";
  protected int numHandlers;
  private String driver;
  private String url;
  private String prefix;

  /**
   * Constructor.
   *
   * @param cf configuration file name
   */
  public VDX(String cf) {
    super();
    name = "VDX";

    LOGGER.info(Version.VERSION_STRING);

    if (cf != null) {
      configFilename = cf;
    }
    processConfigFile();

    for (int i = 0; i < numHandlers; i++) {
      this.addCommandHandler(new ServerHandler(this));
    }

    startListening();
  }

  /**
   * Log fatal error & shut down.
   *
   * @param msg error message
   */
  protected void fatalError(String msg) {
    LOGGER.error(msg);
    System.exit(1);
  }

  /**
   * Process configuration file and fill internal data.
   */
  public void processConfigFile() {
    ConfigFile cf = new ConfigFile(configFilename);
    if (!cf.wasSuccessfullyRead()) {
      fatalError(configFilename + ": could not read config file.");
    }

    int l = StringUtils.stringToInt(cf.getString("vdx.logLevel"), -1);
    if (l > 1) {
      org.apache.log4j.Logger.getRootLogger().setLevel(Level.toLevel("DEBUG"));
    } else {
      org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    }
    LOGGER.info("config: vdx.logLevel={}", l);

    int p = StringUtils.stringToInt(cf.getString("vdx.port"), -1);
    if (p < 0 || p > 65535) {
      fatalError(configFilename + ": bad or missing 'vdx.port' setting.");
    }
    serverPort = p;
    LOGGER.info("config: vdx.port={}", serverPort);

    int h = StringUtils.stringToInt(cf.getString("vdx.handlers"), -1);
    if (h < 1 || h > 128) {
      fatalError(configFilename + ": bad or missing 'vdx.handlers' setting.");
    }
    numHandlers = h;
    LOGGER.info("config: vdx.handlers={}", numHandlers);

    driver = cf.getString("vdx.driver");
    if (driver == null) {
      fatalError(configFilename + ": bad or missing 'vdx.driver' setting.");
    }
    LOGGER.info("config: vdx.driver={}", driver);

    url = cf.getString("vdx.url");
    if (url == null) {
      fatalError(configFilename + ": bad or missing 'vdx.url' setting.");
    }
    LOGGER.info("config: vdx.url={}", url);

    prefix = cf.getString("vdx.prefix");
    if (prefix == null) {
      fatalError(configFilename + ": bad or missing 'vdx.prefix' setting.");
    }
    LOGGER.info("config: vdx.prefix={}", prefix);

    String logFile = cf.getString("vdx.logFile");
    if (logFile != null) {
      try {
        Log.addFileAppender(logFile, "%d{yyyy-MM-dd hh:mm:ss} %5p - %c{1} - %m%n");
      } catch (IOException e) {
        LOGGER.debug("Failed to create log file: {}", logFile, e);
      }
    }

    int m = StringUtils.stringToInt(cf.getString("vdx.maxConnections"), -1);
    if (m < 0) {
      fatalError(configFilename + ": bad or missing 'vdx.maxConnections' setting.");
    }

    connections.setMaxConnections(m);
    LOGGER.info("config: vdx.maxConnections={}", connections.getMaxConnections());
  }

  /**
   * Yield database driver.
   *
   * @return driver
   */
  public String getDbDriver() {
    return driver;
  }

  /**
   * Yield database url.
   *
   * @return url
   */
  public String getDbUrl() {
    return url;
  }

  /**
   * Yield prefix.
   *
   * @return prefix
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * Main method, starts new thread for VDX server which listen configured port, and expect 'q'
   * symbol on stdin to exit.
   *
   * @param args command line args
   */
  public static void main(final String[] args) throws IOException {
    Set arguments = new HashSet<String>();
    for (String arg : args) {
      arguments.add(arg);
    }
    Thread vdxThread = new Thread(new Runnable() {
      public void run() {
        String cf = null;
        if (args.length > 0 && !args[args.length - 1].startsWith("-")) {
          cf = args[args.length - 1];
        }
        new VDX(cf);
      }
    });
    vdxThread.setName("VDX");
    vdxThread.start();
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    boolean acceptCommands = !(arguments.contains("--noinput") || arguments.contains("-i"));
    while (acceptCommands) {
      String s = in.readLine();
      if (s != null) {
        s = s.toLowerCase().trim();
        if (s.equals("q")) {
          System.exit(0);
        }
      }
    }
  }

}
