package gov.usgs.volcanoes.vdx.in.hypo;

import gov.usgs.volcanoes.core.legacy.Arguments;
import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.vdx.data.hypo.Hypocenter;
import gov.usgs.volcanoes.vdx.data.hypo.SQLHypocenterDataSource;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Abstract class to parse given list of files and insert parsed hypocenters
 * into database.
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
abstract public class Importer
{
	protected SQLHypocenterDataSource dataSource;
	protected static Set<String> flags;
	protected static Set<String> keys;
	
	static
	{
		flags = new HashSet<String>();
		keys = new HashSet<String>();
		keys.add("-n");
		keys.add("-c");
	}
	
	/**
	 * Constructor
	 * @param ds SQLHypocenterDataSource to import
	 */
	public Importer(SQLHypocenterDataSource ds)
	{
		dataSource = ds;
	}
	
	/**
	 * Fabric method for data source
	 * @param args command line arguments
	 * @return data source
	 */
	public static SQLHypocenterDataSource getDataSource(Arguments args)
	{
		String cfn = args.get("-c");
		if (cfn == null)
			outputInstructions();
		String name = args.get("-n");
		if (name == null)
			outputInstructions();
		
		SQLHypocenterDataSource ds = new SQLHypocenterDataSource();
		ConfigFile cf = new ConfigFile(cfn);
		// TODO: work out new initialization
		// ds.initialize(cf);
		return ds;
	}
	
	/**
	 * Abstract method to parse data from url (resource locator or file name)
	 * @param resource resource identifier
	 * @return Hypocenters list
	 */
	abstract public List<Hypocenter> importResource(String resource);

	/**
	 * Insert hypocenters list into database
	 * @param hypos list of hypocenters
	 */
	protected void insert(List<Hypocenter> hypos)
	{
		for (Hypocenter hc : hypos)
			dataSource.insertHypocenter(hc);
	}
	
	/**
	 * Print usage instructions on stdout
	 */
	protected static void outputInstructions()
	{
		System.out.println("<importer> -c [vdx config] -n [database name] files...");
		System.exit(-1);
	}
	
	/**
	 * Parse files from command line and insert hypocenters list into database
	 * @param args command line arguments
	 * @param impt importer to process
	 */
	protected static void process(Arguments args, Importer impt)
	{
		if (args.size() == 0)
			outputInstructions();
	
		List<String> resources = args.unused();
		if (resources == null || resources.size() == 0)
		{
			System.out.println("no files");
			System.exit(-1);
		}
		for (String res : resources)
		{
			System.out.println("Reading resource: " + res);
			impt.insert(impt.importResource(res));
		}	
	}
}
