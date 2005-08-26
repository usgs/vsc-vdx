package gov.usgs.vdx.data.hypo;

import gov.usgs.util.Arguments;
import gov.usgs.util.ResourceReader;
import gov.usgs.util.Util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Class for importing UW format catalog files.
 *  
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class ImportUW extends Importer
{
	private SimpleDateFormat dateIn;
	
	public ImportUW(SQLHypocenterDataSource ds)
	{
		super(ds);
		dateIn = new SimpleDateFormat("yyyyMMddHHmm");// ss.SS");
		dateIn.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	public List<Hypocenter> importResource(String resource)
	{
		ResourceReader rr = ResourceReader.getResourceReader(resource);
		if (rr == null)
			return null;
		
		List<Hypocenter> hypos = new ArrayList<Hypocenter>();
		String s;
		int lines = 0;
		while ((s = rr.nextLine()) != null)
		{
			try
			{
				lines++;
				if (s.charAt(0) != 'A')
					throw new Exception("First character not 'A'.");
				
				if (s.length() < 20)
					throw new Exception("Incomplete record.");
				
				Date d = dateIn.parse(s.substring(2, 14));
				double j2ksec = Util.dateToJ2K(d);
				double ds = Double.parseDouble(s.substring(14, 20).trim());
				j2ksec += ds;
				
				double latdeg = Double.parseDouble(s.substring(21, 23).trim());
				char ns = s.charAt(23);
				double latmin = Double.parseDouble(s.substring(24, 29).trim()) / 100;
				double lat = latdeg + latmin / 60.0d;
				if (ns == 'S')
					lat *= -1;
				
				double londeg = Double.parseDouble(s.substring(29, 32).trim());
				char ew = s.charAt(32);
				double lonmin = Double.parseDouble(s.substring(33, 38).trim()) / 100;
				double lon = londeg + lonmin / 60.0d;
				if (ew == 'W')
					lon *= -1;
				
				double depth = -Double.parseDouble(s.substring(38, 43).trim());
				double mag = Double.parseDouble(s.substring(45, 48));
				
				Hypocenter hc = new Hypocenter(new double[] {j2ksec, lon, lat, depth, mag});
				hypos.add(hc);
			}
			catch (Exception e)
			{
				System.err.println("Line " + lines + ": " + e.getMessage());
			}
		}
		return hypos;
	}
	
	public static void main(String as[])
	{
		Arguments args = new Arguments(as, flags, keys);
		SQLHypocenterDataSource ds = Importer.getDataSource(args);
		process(args, new ImportUW(ds));
	}
}