package gov.usgs.vdx.in.generic.variable;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.ResourceReader;
import gov.usgs.vdx.data.generic.variable.DataType;
import gov.usgs.vdx.data.generic.variable.SQLGenericVariableDataSource;
import gov.usgs.vdx.data.generic.variable.Station;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
  * Import NWIS data from url
  * 
  * $Log: not supported by cvs2svn $
  * Revision 1.11  2008/04/07 17:41:20  tparker
  * ignore RDB values with qualification codes
  *
  * Revision 1.10  2007/03/19 01:14:31  tparker
  * cleanup
  *
  * Revision 1.9  2007/02/01 20:22:44  tparker
  * correct axis labeling
  *
  * Revision 1.8  2006/09/20 23:30:19  tparker
  * only import active stations
  *
  * Revision 1.7  2006/09/20 22:51:34  tparker
  * Fix null reading bug
  *
  * Revision 1.6  2006/09/20 22:35:48  tparker
  * Allow for sparse results from NWIS
  *
  * Revision 1.5  2006/09/20 21:17:38  tparker
  * rewrite rdb parser
  *
  * Revision 1.4  2006/09/15 00:33:27  tparker
  * update pattern for NWIS matching
  *
  * Revision 1.3  2006/08/28 23:58:42  tparker
  * Initial NWIS commit
  *
  * Revision 1.2  2006/08/01 21:36:29  tparker
  * Cleanup imports
  *
  * Revision 1.1  2006/08/01 19:54:47  tparker
  * Create NWIS data source
  *
 *
 * @author Tom Parker
 */
public class ImportNWIS
{	
	private static final String CONFIG_FILE = "NWIS.config";
	private Logger logger;
	private List<Station> stations;
	private SQLGenericVariableDataSource dataSource;
	private ConfigFile params;

	/**
	 * Constructor
	 * @param cf configuration file to specify data source to import in and data structure
	 */
	public ImportNWIS(String cf)
	{
		dataSource = new SQLGenericVariableDataSource();
		params = new ConfigFile(cf);
		
		// TODO: work out new initialization
		// dataSource.initialize(params);
		//dataSource.setName(params.getString("vdx.name"));
		
		// this is commented out until i figure out what to do with it  (LJA)
		// stations = dataSource.getStations();
	}	
	
	/**
	 * Import NWIS data from url ('url' parameter in the configuration)
	 * @param st station
	 * @param period not used really
	 */
	public void importWeb(Station st, int period)
	{
		if (!st.getActive())
			return;
		
		List<DataType> dataTypes = new ArrayList<DataType>();
		logger = Log.getLogger("gov.usgs.vdx");
		String fn = params.getString("url") + "&period=" + period + "&site_no=" + st.getSiteNo();
		
		try
		{
			ResourceReader rr = ResourceReader.getResourceReader(fn);
			if (rr == null)
				return;
			logger.info("importing: " + fn);
			SimpleDateFormat dateIn;
			
			dateIn = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			dateIn.setTimeZone(TimeZone.getTimeZone(st.getTz()));
			
			String s = rr.nextLine();
			Pattern p;
			Matcher m;
			
			// match header
			//p = Pattern.compile("^#\\s+\\*(\\d+)\\s+(\\d+)\\s+-\\s+(.*)$");
			p = Pattern.compile("^#\\s+(\\d+)\\s+(\\d+)\\s+(.*)$");
			Pattern p1 = Pattern.compile("^#.*$");
			while (s != null && p1.matcher(s).matches())
			{
				m = p.matcher(s);
				if (m.matches())
				{
					int dataType = Integer.parseInt(m.group(2));
					String name = m.group(3);
					dataTypes.add(new DataType(dataType, name));
				}
				
				s = rr.nextLine();
			}
			
			// parse column name row 
			String[] ss = s.split("\t");
			for (int i = 0; i < dataTypes.size(); i++)
			{
				int index = i * 2 + 3;
				int id = Integer.parseInt(ss[index].substring(3));
				
				if (dataTypes.get(i).getId() != id)
				{
					DataType t;
					for (int j = i; j < dataTypes.size(); j++)
					{
						if (dataTypes.get(j).getId() == id)
						{
							t = dataTypes.get(i);
							dataTypes.set(i, dataTypes.get(j));
							dataTypes.set(j, t);
						}
						
					}
				}										
				i++; // discard _cd column
			}
			
			s = rr.nextLine(); // discard collumn definition row

			// match records
			s = rr.nextLine();
			while (s != null)
			{
				ss = s.split("\t", -1);
				
				// assume midnight if no time given
				if (!ss[2].contains(" "))
					ss[2] += " 00:00";
				
				Date date = dateIn.parse(ss[2]);
				for (int i=0; i < dataTypes.size(); i++)
				{
					int index = i * 2 + 3;
					// ignore values that are empty or have embeded qualification codes
					if (ss[index].length() > 0 && ss[index].indexOf('_') == -1)
						dataSource.insertRecord(date, st, dataTypes.get(i), Double.parseDouble(ss[index]));
					else
						System.out.println ("skipping " + ss[index] + " idex: " + ss[index].indexOf('_'));
				s = rr.nextLine();
				}
			}
			
			for (DataType dt : dataTypes)
				dataSource.insertDataType(dt);
			
			System.out.println();
		}
		catch (Exception e)
		{
			e.printStackTrace();	
		}
	}

	/**
	 * Main method
	 * @param as command line args
	 */
	public static void main(String[] as)
	{
		String cf = CONFIG_FILE;
		int period = 1;
		Set<String> flags;
		Set<String> keys;

		flags = new HashSet<String>();
		keys = new HashSet<String>();
		keys.add("-c");
		keys.add("-h");
		keys.add("-p");

		Arguments args = new Arguments(as, flags, keys);
		
		if (args.contains("-h"))
		{
			System.err.println("java gov.usgs.vdx.data.gps.ImportNWIS [-c configFile] [-p period]");
			System.exit(-1);
		}
		
		if (args.contains("-c"))
			cf = args.get("-c");

		
		if (args.contains("-p"))
			period = Integer.parseInt(args.get("-p"));
		
		ImportNWIS in = new ImportNWIS(cf);
		List<String> files = args.unused();

		for (Station station : in.stations)
			in.importWeb(station, period);

	}	
}
