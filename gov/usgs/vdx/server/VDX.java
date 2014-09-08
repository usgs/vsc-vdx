package gov.usgs.vdx.server;

import gov.usgs.net.Server;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.Util;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.logging.Level;

/**
 * Main class for VDX server
 *
 * @author Dan Cervelli
 */
public class VDX extends Server {
	protected String configFilename = "VDX.config";
	protected int numHandlers;
	private String driver;
	private String url;
	private String prefix;

	/**
	 * Constructor
	 * @param cf configuration file name
	 */	
	public VDX(String cf) {
		super();
		name	= "VDX";
		logger	= Log.getLogger("gov.usgs.vdx");

		String[] version = Util.getVersion("gov.usgs.vdx");
		if (version != null) {
			logger.info("Version: " + version[0] + " Built: " + version[1]);
		} else {
			logger.info("No version information available.");
		}
		
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
	 * Log fatal error & shut down
	 * @param msg error message
	 */
	protected void fatalError(String msg) {
		logger.severe(msg);
		System.exit(1);
	}
	
	/**
	 * Process configuration file and fill internal data
	 */
	public void processConfigFile() {
		ConfigFile cf = new ConfigFile(configFilename);
		if (!cf.wasSuccessfullyRead())
			fatalError(configFilename + ": could not read config file.");

		int l = Util.stringToInt(cf.getString("vdx.logLevel"), -1);
		if (l > 1)
			logger.setLevel(Level.ALL);
		else
			logger.setLevel(Level.INFO);
		logger.info("config: vdx.logLevel=" + l + ".");

		int p = Util.stringToInt(cf.getString("vdx.port"), -1);
		if (p < 0 || p > 65535)
			fatalError(configFilename + ": bad or missing 'vdx.port' setting.");
		serverPort = p;
		logger.info("config: vdx.port=" + serverPort + ".");

		int h = Util.stringToInt(cf.getString("vdx.handlers"), -1);
		if (h < 1 || h > 128)
			fatalError(configFilename + ": bad or missing 'vdx.handlers' setting.");
		numHandlers = h;
		logger.info("config: vdx.handlers=" + numHandlers + ".");

		driver = cf.getString("vdx.driver");
		if (driver == null)
			fatalError(configFilename + ": bad or missing 'vdx.driver' setting.");
		logger.info("config: vdx.driver=" + driver + ".");

		url = cf.getString("vdx.url");
		if (url == null)
			fatalError(configFilename + ": bad or missing 'vdx.url' setting.");
		logger.info("config: vdx.url=" + url + ".");

		prefix = cf.getString("vdx.prefix");
		if (prefix == null)
			fatalError(configFilename + ": bad or missing 'vdx.prefix' setting.");
		logger.info("config: vdx.prefix=" + prefix + ".");

		int m = Util.stringToInt(cf.getString("vdx.maxConnections"), -1);
		if (m < 0)
			fatalError(configFilename + ": bad or missing 'vdx.maxConnections' setting.");

		maxConnections = m;
		logger.info("config: vdx.maxConnections=" + maxConnections + ".");
	}

	/**
	 * Yield database driver
	 * @return driver
	 */
	public String getDbDriver() {
		return driver;
	}
	
	/**
	 * Yield database url
	 * @return url
	 */
	public String getDbUrl() {
		return url;
	}
	
	/**
	 * Yield prefix
	 * @return prefix
	 */
	public String getPrefix() {
		return prefix;
	}

	/**
	 * Main method, 
	 * starts new thread for VDX server which listen configured port,
	 * and expect 'q' symbol on stdin to exit.
	 * @param args command line args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		Set arguments = Util.toSet(args);
		Thread vdxThread = new Thread(new Runnable()
				{
					public void run()
					{
				String cf = null;
				if (args.length > 0 && !args[args.length - 1].startsWith("-"))
					cf = args[args.length - 1];
				new VDX(cf);
			}
		});
		vdxThread.setName("VDX");
		vdxThread.start();
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		boolean acceptCommands = !(arguments.contains("--noinput") || arguments.contains("-i"));
		while (acceptCommands)
		{
			String s = in.readLine();
			if (s != null)
			{
				s = s.toLowerCase().trim();
				if (s.equals("q"))
					System.exit(0);
			}
		}
	}

}
