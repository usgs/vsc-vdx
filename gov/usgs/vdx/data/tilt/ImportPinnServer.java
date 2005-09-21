package gov.usgs.vdx.data.tilt;

import gov.usgs.pinnacle.Client;
import gov.usgs.pinnacle.StatusBlock;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.Util;
import gov.usgs.vdx.db.VDXDatabase;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class ImportPinnServer extends Client
{
	private static final String CONFIG_FILE = "PinnClient.config";
	private SQLTiltDataSource dataSource;
	private String channel;
	private Logger logger;
	
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
		
		String driver = cf.getString("vdx.driver");
		String url = cf.getString("vdx.url");
		String prefix = cf.getString("vdx.prefix");
		String name = cf.getString("vdx.name");
		
		ips.dataSource = new SQLTiltDataSource();
		
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
		
		if (!ips.dataSource.defaultChannelExists("tilt", ips.channel))
		{
			if (ips.dataSource.createChannel(ips.channel, ips.channel, -999, -999))
				ips.logger.info("created channel.");
		}
		return ips;
	}

	public void handleStatusBlock(StatusBlock sb)
	{
		System.out.println(sb);
		dataSource.insertData(channel, sb.getJ2K(), sb.getXMillis(), sb.getYMillis(), 0, 1, 1, 0, 0);
	}
	
	public static void main(String[] args)
	{
		ImportPinnServer ips = ImportPinnServer.createImportPinnServer(null);
		ips.startListening();
	}
}
