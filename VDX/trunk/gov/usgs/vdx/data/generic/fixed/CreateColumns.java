package gov.usgs.vdx.data.generic.fixed;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class for importing CSV format files.
 *  
 * @author Tom Parker
 */
public class CreateColumns
{

	protected SQLGenericFixedDataSource dataSource;
	protected static Set<String> flags;
	protected static Set<String> keys;
	private String channel, channelName;
	private Double lon, lat;
	private int index;
	private String column, description, unit;
	private boolean checked, active;
	private ConfigFile params;
	private ConfigFile vdxParams;
	private String vdxName;
	protected Logger logger;

	/**
	 * Constructor
	 * @param cf configuration file to specify data source to import in and data structure
	 */
	public CreateColumns(String cf) {
		
		// setup the logger and build up a structure for the config file
		logger		= Log.getLogger("gov.usgs.vdx");
		params		= new ConfigFile(cf);
		
		// get the parameters from the config file
		processConfigFile();
	}
	
	/**
	 * Parse configuration and init class object
	 */
	private void processConfigFile() {
		
		ConfigFile sub;
		vdxParams	= new ConfigFile(params.getString("vdxConfig"));
		vdxName		= params.getString("vdx.name");
		vdxParams.put("vdx.name", vdxName);
		
		// create a connection to the database using values from the config file
		dataSource	= new SQLGenericFixedDataSource();
		dataSource.initialize(vdxParams);
		
		// get the list of columns
		List<String> columns = params.getList("column");		
		Iterator<String> coit = columns.iterator();	
		
		// create a data structure for the columns
		while (coit.hasNext()) {
			
			column		= coit.next();
			sub			= params.getSubConfig(column);
			index		= Integer.parseInt(sub.getString("index"));
			description	= sub.getString("description");
			unit		= sub.getString("unit");
			checked		= sub.getString("checked").equals("1");
			active		= sub.getString("active").equals("1");
			
			GenericColumn gc = new GenericColumn(index, column, description, unit, checked, active);
			dataSource.createColumn(gc);
		}		
		
		// get the list of channels
		List<String> channels	= params.getList("channel");
		Iterator<String> chit	= channels.iterator();
		
		// iterate over each of the channels and create a table for it
		while (chit.hasNext()) {
			
			channel		= chit.next();
			sub			= params.getSubConfig(channel);
			channelName	= sub.getString("name"); 
			lon			= Double.parseDouble(sub.getString("lon"));
			lat			= Double.parseDouble(sub.getString("lat"));	
			
			dataSource.createChannel(channel, channelName, lon, lat);
		}
	}
	
	/**
	 * Main method
	 * Command line syntax:
	 *  -c config file name
	 *  -h, --help print help message
	 *  -v verbose mode
	 */
	public static void main(String as[])
	{
		Logger 	logger = Log.getLogger("gov.usgs.vdx");
		logger.setLevel(Level.INFO);

		String cf = null;
		Set<String> flags;
		Set<String> keys;
		
		flags = new HashSet<String>();
		keys = new HashSet<String>();
		keys.add("-c");
		flags.add("-h");
		flags.add("--help");
		flags.add("-v");
		
		Arguments args = new Arguments(as, flags, keys);
		
		if (args.flagged("-h") || args.flagged("--help")) {
			System.err.println("java gov.usgs.vdx.data.generic.CreateColumns [-c configFile] [-g]");
			System.exit(-1);
		}
		
		if (args.contains("-c"))
			cf = args.get("-c");
		
		if (args.flagged("-v"))
			logger.setLevel(Level.ALL);
		
		CreateColumns cc = new CreateColumns(cf);
	}
}

