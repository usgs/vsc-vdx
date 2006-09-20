package gov.usgs.vdx.data.nwis;

import gov.usgs.util.Arguments;
import gov.usgs.util.ConfigFile;
import gov.usgs.util.Log;
import gov.usgs.util.ResourceReader;

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
 *
  * $Log: not supported by cvs2svn $
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
	private SQLNWISDataSource dataSource;
	private ConfigFile params;

	public ImportNWIS(String cf)
	{
		dataSource = new SQLNWISDataSource();
		params = new ConfigFile(cf);
		
		dataSource.initialize(params);
		//dataSource.setName(params.getString("vdx.name"));
		stations = dataSource.getStations();
	}
	
	public void importWeb2(Station st, int period)
	{
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
			boolean next = false;
			Pattern p;
			Matcher m;
			
			// match header
			//p = Pattern.compile("^#\\s+\\*(\\d+)\\s+(\\d+)\\s+-\\s+(.*)$");
			p = Pattern.compile("^#\\s+(\\d+)\\s+(\\d+)\\s+(.*)$");
			Pattern p1 = Pattern.compile("^#.*$");
			while (s != null && next == false)
			{
				m = p.matcher(s);
				if (m.matches())
				{
					int dataType = Integer.parseInt(m.group(2));
					String name = m.group(3);
					dataTypes.add(new DataType(dataType, name));
				}
				
				s = rr.nextLine();
				Matcher m1 = p1.matcher(s);
				if (!m1.matches())
					next = true;
			}
			next = false;
			
			//match key
			String pattern = "^agency_cd\\s+site_no\\s+datetime";
			for (int i = 0; i < dataTypes.size(); i++)
				pattern += "\\s+\\d{2}_(\\d{5})\\s+\\d{2}_\\d{5}_cd";
			pattern += ".*$";
			p = Pattern.compile(pattern);
			
			while(s != null && next == false)
			{
				m = p.matcher(s);
				
				if (m.matches())
				{
					for (int i = 0; i < dataTypes.size(); i++)
					{
						int id = Integer.parseInt(m.group(i+1));
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
					}
					next = true;
				}
				s = rr.nextLine();
			}
			
			// match records
			next = false;
			pattern = "^" + st.getOrg() + "\\s+" + st.getSiteNo() + "\\s+";
			pattern += "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2})\\s+";
			for (int i = 0; i < dataTypes.size(); i++)
				pattern += "([\\d.]+)\\s+";

			Pattern recordPattern = Pattern.compile(pattern);

			while (s != null)
			{
				Matcher recordMatcher = recordPattern.matcher(s);
				
				if (recordMatcher.matches())
				{
					Date date = dateIn.parse(recordMatcher.group(1));
					for (int i = 0; i < dataTypes.size(); i++)
					{
						double reading = Double.parseDouble(recordMatcher.group(i+2));
						dataSource.insertRecord(date, st, dataTypes.get(i), reading);
					}
				} else {
					System.out.println("Looking for " + pattern);
					System.out.println("no match on " + s);
				}
				s = rr.nextLine();
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
	
	public void importWeb(Station st, int period)
	{
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
					String r = (ss[index].length() > 0) ? ss[index] : "0";
					
					double reading = Double.parseDouble(r);
					dataSource.insertRecord(date, st, dataTypes.get(i), reading);
				}
				s = rr.nextLine();
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
		for (Station station : in.stations)
			in.importWeb(station, period);

	}	
}
