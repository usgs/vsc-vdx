package gov.usgs.vdx.data.tilt;

import gov.usgs.pinnacle.Client;
import gov.usgs.pinnacle.StatusBlock;
import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;
import gov.usgs.vdx.db.VDXDatabase;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Logger;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.4  2005/10/20 05:06:06  dcervelli
 * Fixed default xMult, yMult values.
 *
 * Revision 1.3  2005/10/14 20:44:29  dcervelli
 * Added calibration.
 *
 * Revision 1.2  2005/10/13 22:18:14  dcervelli
 * Changes for etilt.
 *
 * Revision 1.1  2005/09/21 19:25:14  dcervelli
 * Initial commit.
 *
 * @author Dan Cervelli
 */
public class ImportPinnServer extends Client
{
	private static final String CONFIG_FILE = "PinnClient.config";
	private SQLElectronicTiltDataSource dataSource;
	private String channel;
	private Logger logger;
	
	private double azimuth = 0;
	private double xMult = 1;
	private double yMult = 1;
	private double xOffset = 0;
	private double yOffset = 0;
	
		
	public ImportPinnServer(String h, int p)
	{
		super(h, p);
		logger = Log.getLogger("gov.usgs.vdx");
	}
	
	public static ImportPinnServer createImportPinnServer(String fn)
	{
		if (fn == null)
			fn = CONFIG_FILE;
		ConfigFile cf = new ConfigFile(fn);
		String host = cf.getString("server.host");
		int port = Util.stringToInt(cf.getString("server.port"), 17000);
		ImportPinnServer ips = new ImportPinnServer(host, port);

		ips.channel = cf.getString("channel");
		ips.azimuth = Util.stringToDouble(cf.getString("azimuth"), 0);
		ips.xMult = Util.stringToDouble(cf.getString("xMult"), 1);
		ips.yMult = Util.stringToDouble(cf.getString("yMult"), 1);
		ips.xOffset = Util.stringToDouble(cf.getString("xOffset"), 0);
		ips.yOffset = Util.stringToDouble(cf.getString("yOffset"), 0);
		
		String driver = cf.getString("vdx.driver");
		String url = cf.getString("vdx.url");
		String prefix = cf.getString("vdx.prefix");
		String name = cf.getString("vdx.name");
		
		ips.dataSource = new SQLElectronicTiltDataSource();
		
		VDXDatabase database = new VDXDatabase(driver, url, prefix);
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("VDXDatabase", database);
		params.put("name", name);
		ips.dataSource.initialize(params);
		
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

	public void handleStatusBlock(StatusBlock sb)
	{
		System.out.println(sb);
		dataSource.insertData(channel, sb.getJ2K(), sb.getXMillis(), sb.getYMillis(), sb.getVoltage(), sb.getTemperature(), azimuth, xMult, yMult, xOffset, yOffset, 1, 0, 1, 0);
	}
	
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
