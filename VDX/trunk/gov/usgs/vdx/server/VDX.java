package gov.usgs.vdx.server;

import gov.usgs.net.Server;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class VDX extends Server
{
	protected String configFilename = "VDX.config";
	protected int numHandlers;
	
	public VDX(String cf)
	{
		super();
		name = "VDX";
		logger = Log.getLogger("gov.usgs.vdx");

		String[] version = Util.getVersion("gov.usgs.vdx");
		if (version != null)
			logger.info("Version: " + version[0] + " Built: " + version[1]);
		else
			logger.info("No version information available.");
		
		if (cf != null)
			configFilename = cf;
		processConfigFile();
		for (int i = 0; i < numHandlers; i++)
			this.addReadHandler(new ServerHandler(this));

		startListening();
	}
	
	protected void fatalError(String msg)
	{
		logger.severe(msg);
		System.exit(1);
	}
	
	public void processConfigFile()
	{
		ConfigFile cf = new ConfigFile(configFilename);
		if (!cf.wasSuccessfullyRead())
			fatalError(configFilename + ": could not read config file.");
		
		int p = Util.stringToInt(cf.getString("vdx.port"), -1);
		if (p < 0 || p > 65535)
			fatalError(configFilename + ": bad or missing 'vdx.port' setting.");
		port = p;
		logger.info("config: vdx.port=" + port + ".");

		int h = Util.stringToInt(cf.getString("vdx.handlers"), -1); 
		if (h < 1 || h > 128)
			fatalError(configFilename + ": bad or missing 'vdx.handlers' setting.");
		numHandlers = h;
		logger.info("config: vdx.handlers=" + numHandlers + ".");

		int m = Util.stringToInt(cf.getString("vdx.maxConnections"), -1);
		if (m < 0)
			fatalError(configFilename + ": bad or missing 'vdx.maxConnections' setting.");
			
		maxConnections = m;
		logger.info("config: vdx.maxConnections=" + maxConnections + ".");
	}
	
	public static void main(final String[] args) throws IOException
	{
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
