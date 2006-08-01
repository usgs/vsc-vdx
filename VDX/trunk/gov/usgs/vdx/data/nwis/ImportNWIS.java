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
import java.util.TimeZone;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
  * $Log: not supported by cvs2svn $
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
		dataSource.setName(params.getString("vdx.name"));
		stations = dataSource.getStations();
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
			while (s != null)
			{
				Pattern typePattern = Pattern.compile("^#\\s+\\*(\\d+)\\s+(\\d+)\\s+-\\s+(.*)$");
				Matcher typeMatcher = typePattern.matcher(s);
				
				String pattern = "^" + st.getOrg() + "\\s+" + st.getSiteNo() + "\\s+";
				pattern += "(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2})\\s+";
				for (int i = 0; i < dataTypes.size(); i++)
					pattern += "([\\d.]+)\\s+";

				Pattern recordPattern = Pattern.compile(pattern);
				Matcher recordMatcher = recordPattern.matcher(s);
				
				if (typeMatcher.matches())
				{
					int dataType = Integer.parseInt(typeMatcher.group(2));
					String name = typeMatcher.group(3);
					dataTypes.add(new DataType(dataType, name));
				}
				else if (recordMatcher.matches())
				{
					Date date = dateIn.parse(recordMatcher.group(1));
					for (int i = 0; i < dataTypes.size(); i++)
					{
						double reading = Double.parseDouble(recordMatcher.group(i+2));
						dataSource.insertRecord(date, st, dataTypes.get(i), reading);
					}
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
