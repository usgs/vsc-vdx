package gov.usgs.vdx.data.tilt;

import gov.usgs.pinnacle.Client;
import gov.usgs.pinnacle.StatusBlock;
import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;
import gov.usgs.vdx.db.VDXDatabase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Import pinnacle data - from pinnacle server socket or from files
 * 
 * @author Dan Cervelli
 */
public class ImportPinnServer extends Client
{
	private static final String CONFIG_FILE = "PinnClient.config";
	private SQLTiltStationDataSource dataSource;
	private String channel;
	private Logger logger;
	
		
	
	/**
	 * Constructor
	 * @param h host 
	 * @param p port
	 */
	public ImportPinnServer(String h, int p)
	{
		super(h, p);
		logger = Log.getLogger("gov.usgs.vdx");
	}
	
	/**
	 * Fabric method to create instance
	 * @param fn configuration file name
	 */
	public static ImportPinnServer createImportPinnServer(String fn)
	{
		if (fn == null)
			fn = CONFIG_FILE;
		ConfigFile cf = new ConfigFile(fn);
		String host = cf.getString("server.host");
		int port = Util.stringToInt(cf.getString("server.port"), 17000);
		ImportPinnServer ips = new ImportPinnServer(host, port);

		ips.channel = cf.getString("channel");
		
		String driver = cf.getString("vdx.driver");
		String url = cf.getString("vdx.url");
		String vdxPrefix = cf.getString("vdx.vdxPrefix");
		if (vdxPrefix == null)
			throw new RuntimeException("can't find config parameter vdx.vdxPrefix. Update config file if using vdx.prefix");

		String name = cf.getString("vdx.name");
		
		ips.dataSource = new SQLTiltStationDataSource();
		
		VDXDatabase database = new VDXDatabase(driver, url, vdxPrefix);
		ips.dataSource.setDatabase(database);
		ips.dataSource.setName(name);
		
		if (!ips.dataSource.databaseExists())
		{
			if (ips.dataSource.createDatabase())
				ips.logger.info("created database.");
		}
		
		if (!ips.dataSource.defaultChannelExists("etilt", ips.channel))
		{
			if (ips.dataSource.createChannel(ips.channel, ips.channel, -999, -999))
				ips.logger.info("created channel.");
		}
		return ips;
	}

	/**
	 * Method to handle with data block which was got from pinnacle server
	 * @see gov.usgs.pinnacle.Client
	 */
	public void handleStatusBlock(StatusBlock sb)
	{
		System.out.println(sb);
		dataSource.insertData(channel, sb.getJ2K(), sb.getXMillis(), sb.getYMillis(), sb.getVoltage(), 0, sb.getTemperature(), 0, 0);
	}
	
	/**
	 * Import from file which contains pinnacle blocks
	 * @param fn file name
	 */
	public void importFile(String fn)
	{
		logger = Log.getLogger("gov.usgs.vdx");
		
		try
		{
			ResourceReader rr = ResourceReader.getResourceReader(fn);
			if (rr == null)
				return;
			logger.info("importing: " + fn);

			String s = rr.nextLine();
			while (s != null)
			{
				if (s.substring(21,24).equals("SB:"))
				{
					StatusBlock sb = new StatusBlock(Util.hexToBytes(s.substring(25)));
					handleStatusBlock(sb);
				}
				s = rr.nextLine();
			}		
		} 
		catch (Exception e)
		{
			e.printStackTrace();	
		}
	}
	
	/**
	 * Main method
	 * Syntax is:
	 * java gov.usgs.vdx.data.tilt.ImportPinnServer [-c <configFile>] [files...]
	 * 
	 * If file names are given than import files, else connect and listen for pinnacle server
	 */
	public static void main(String[] as)
	{
		String cf = null;
		Set<String> flags;
		Set<String> keys;

		flags = new HashSet<String>();
		keys = new HashSet<String>();
		keys.add("-c");

		Arguments args = new Arguments(as, flags, keys);
		
		if (args.contains("-c"))
			cf = args.get("-c");
		
		List<String> unusedArgs = args.unused();
		ImportPinnServer ips = ImportPinnServer.createImportPinnServer(cf);
		
		if (unusedArgs.size() > 0) 
		{
			for (String file : unusedArgs)
				ips.importFile(file);			
		}
		else
			ips.startListening();
	}
}
