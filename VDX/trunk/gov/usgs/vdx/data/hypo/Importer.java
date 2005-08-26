package gov.usgs.vdx.data.hypo;

import gov.usgs.util.Arguments;
import gov.usgs.vdx.db.VDXDatabase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	
	public Importer(SQLHypocenterDataSource ds)
	{
		dataSource = ds;
	}
	
	public static SQLHypocenterDataSource getDataSource(Arguments args)
	{
		String cfn = args.get("-c");
		if (cfn == null)
			outputInstructions();
		String name = args.get("-n");
		if (name == null)
			outputInstructions();
		
		SQLHypocenterDataSource ds = new SQLHypocenterDataSource();
		VDXDatabase database = VDXDatabase.getVDXDatabase(cfn);
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("VDXDatabase", database);
		params.put("name", name);
		ds.initialize(params);
		return ds;
	}
	
	abstract public List<Hypocenter> importResource(String resource);

	protected void insert(List<Hypocenter> hypos)
	{
		for (Hypocenter hc : hypos)
			dataSource.insertHypocenter(hc);
	}
	
	protected static void outputInstructions()
	{
		System.out.println("<importer> -c [vdx config] -n [database name] files...");
		System.exit(-1);
	}
	
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
