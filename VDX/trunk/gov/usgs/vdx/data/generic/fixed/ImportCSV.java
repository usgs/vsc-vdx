package gov.usgs.vdx.data.generic.fixed;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.GenericDataMatrix;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class for importing CSV format files.
 *  
 * @author Tom Parker
 */
public class ImportCSV
{

	protected SQLGenericFixedDataSource dataSource;
	protected static Set<String> flags;
	protected static Set<String> keys;
	private SimpleDateFormat dateIn;
	private int timeZoneIndex;
	private int headerRows;
	private String table;
	private Double lon, lat;
	private static final String CONFIG_FILE = "importGenericCSV.config";
	private ConfigFile params;
	private ConfigFile vdxParams;
	private List<GenericColumn> fileCols, dbCols;
	protected Logger logger;

	public ImportCSV(String cf)
	{
		logger = Log.getLogger("gov.usgs.vdx");
		params = new ConfigFile(cf);
		logger.finer("Processing config file");
		processConfigFile();
		dataSource = new SQLGenericFixedDataSource();
		logger.finer("initalizing VDX params");
		dataSource.initialize(vdxParams);
		dbCols = dataSource.getColumns();
		logger.finest("exiting constructor");
	}
	
	private void processConfigFile()
	{
		ConfigFile sub;
		fileCols = new ArrayList<GenericColumn>();
		
		vdxParams = new ConfigFile(params.getString("vdxConfig"));
		headerRows = Integer.parseInt(params.getString("headerRows"));
		table = params.getString("channel");
	
		sub = params.getSubConfig(table);
		lon = Double.parseDouble(sub.getString("lon"));
		lat = Double.parseDouble(sub.getString("lat"));
		
		sub = params.getSubConfig("tz");
		timeZoneIndex = Integer.parseInt(sub.getString("index"));
		dateIn = new SimpleDateFormat(sub.getString("format"));
		dateIn.setTimeZone(TimeZone.getTimeZone(sub.getString("zone")));		
		
		List<String> columns = params.getList("column");
		
		Iterator<String> it = columns.iterator();
		while (it.hasNext())
		{
			String column = it.next();
			logger.finest("found column: " + column);
			sub = params.getSubConfig(column);
			int index = Integer.parseInt(sub.getString("index"));
			String description = sub.getString("description");
			String unit = sub.getString("unit");
			boolean checked = sub.getString("checked").equals("1");
			boolean active = sub.getString("active").equals("1");
			GenericColumn gc = new GenericColumn(index, column, description, unit, checked, active);
			fileCols.add(gc);
		}
	}
	
	public void process(String f)
	{
		logger.fine("processing " + f);
		List<double[]> pts = new ArrayList<double[]>();
		
		try
		{
			ResourceReader rr = ResourceReader.getResourceReader(f);
			if (rr == null)
				return;
			logger.info("importing: " + f);
			
			String line = rr.nextLine();

			for (int i=0; i<headerRows; i++)
				line = rr.nextLine();
			
			while (line != null)
			{
				String[] s = line.split(",");
				logger.info("timestamp " + s[timeZoneIndex]);
				int i=0;
				double[] d = new double[fileCols.size() + 1];

				Date date = dateIn.parse(s[timeZoneIndex]);
				d[i++] = Util.dateToJ2K(date);
				
				for (GenericColumn c : fileCols) {
					logger.info(c.description + " = " + s[c.index]);
						d[i++] = Double.parseDouble(s[c.index]);
				}
				
				pts.add(d);
				line = rr.nextLine();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();	
		}

		GenericDataMatrix gd = new GenericDataMatrix(pts);
		ArrayList<String> colNames = new ArrayList<String>();
		colNames.add("t");
		for (GenericColumn c : fileCols)
			colNames.add(c.name);
		gd.setColumnNames(colNames.toArray(new String[0]));
		
		dataSource.insertData(table, gd);
	}

	protected static void outputInstructions()
	{
		System.out.println("<importer> -c [vdx config] -n [database name] files...");
		System.exit(-1);
	}
	
	protected static void process(Arguments args, SQLGenericFixedDataSource ds)
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
			//ds.insert(ds.importResource(res));
		}	
	}
	
	public List<GenericColumn> importResource(String resource)
	{
		ResourceReader rr = ResourceReader.getResourceReader(resource);
		if (rr == null)
			return null;
		
		List<GenericColumn> hypos = new ArrayList<GenericColumn>();
		String s;
		int lines = 0;
		while ((s = rr.nextLine()) != null)
		{
			try
			{
				lines++;
				
				// DATE
				String year = s.substring(0,4);
				String monthDay = s.substring(4,8);
				
				if (!s.substring(8,9).equals(" "))
					throw new Exception("corrupt data at column 9");
				
				String hourMin = s.substring(9,13);
				String sec = s.substring(13,19).trim();
				
				Date date = dateIn.parse(year+monthDay+hourMin+sec);
				double j2ksec = Util.dateToJ2K(date);

				// LAT
				double latdeg = Double.parseDouble(s.substring(19, 22).trim());
				double latmin = Double.parseDouble(s.substring(23, 28).trim());
				double lat = latdeg + latmin / 60.0d;
				char ns = s.charAt(22);
				if (ns == 'S')
					lat *= -1;


				// LON
				double londeg = Double.parseDouble(s.substring(28, 32).trim());
				char ew = s.charAt(32);
				double lonmin = Double.parseDouble(s.substring(33, 38).trim());
				double lon = londeg + lonmin / 60.0d;
				if (ew != 'W')
					lon *= -1;
				
				// DEPTH
				double depth = -Double.parseDouble(s.substring(38, 45).trim());
				
				// MAGNITUDE
				double mag = Double.parseDouble(s.substring(47, 52).trim());
				
				if (!s.substring(45,46).equals(" "))
					throw new Exception("corrupt data at column 46");

				System.out.println("HC: " + j2ksec + " : " + lon + " : " + lat + " : " + depth + " : " + mag);
				//GenericColumn hc = new GenericColumn(new double[] {j2ksec, lon, lat, depth, mag});
				//hypos.add(hc);
			}
			catch (Exception e)
			{
				System.err.println("Line " + lines + ": " + e.getMessage());
			}
		}
		rr.close();
		return hypos;
	}
	
	public void create ()
	{
		logger.info("Creating channel " + table);
		dataSource.createChannel(table, table, lon, lat);
	}
	public static void main(String as[])
	{
		Logger 	logger = Log.getLogger("gov.usgs.vdx");
		logger.setLevel(Level.INFO);
		
		String cf = CONFIG_FILE;
		Set<String> flags;
		Set<String> keys;
		boolean createDatabase=false;
		
		flags = new HashSet<String>();
		keys = new HashSet<String>();
		keys.add("-c");
		flags.add("-h");
		flags.add("--help");
		flags.add("-g");
		flags.add("--generate");
		flags.add("-v");
		
		Arguments args = new Arguments(as, flags, keys);
		
		if (args.flagged("-h") || args.flagged("--help"))
		{
			System.err.println("java gov.usgs.vdx.data.generic.ImportCSV [-c configFile] [-g]");
			System.exit(-1);
		}
		
		if (args.flagged("-g") || args.flagged("--generate"))
			createDatabase=true;
		
		if (args.contains("-c"))
			cf = args.get("-c");
		
		if (args.flagged("-v"))
			logger.setLevel(Level.ALL);
		
		ImportCSV in = new ImportCSV(cf);
		List<String> files = args.unused();

		if (createDatabase)
			in.create();
		
		for (String file : files)
			in.process(file);
	}
}

