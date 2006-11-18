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
 * Class for importing hypo71 format catalog files.
 *  
 * $Log: not supported by cvs2svn $
 *
 *
 *
 * 20060913 1910 59.02 19 59.26 155 48.83   5.35 D 1.70  4 143 24.  0.01  1.0  2.1 CW      20363
 * @author Dan Cervelli
 */
public class ImportH71 extends Importer
{
	private SimpleDateFormat dateIn;
	
	public ImportH71(SQLHypocenterDataSource ds)
	{
		super(ds);
		dateIn = new SimpleDateFormat("yyyyMMddHHmmsss.SS");// ss.SS");
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
				double latmin = Double.parseDouble(s.substring(23, 28).trim()) / 100;
				double lat = latdeg + latmin / 60.0d;
				char ns = s.charAt(22);
				if (ns == 'S')
					lat *= -1;


				// LON
				double londeg = Double.parseDouble(s.substring(28, 32).trim());
				char ew = s.charAt(32);
				double lonmin = Double.parseDouble(s.substring(33, 38).trim()) / 100;
				double lon = londeg + lonmin / 60.0d;
				if (ew == 'W')
					lon *= -1;
				
				// DEPTH
				double depth = -Double.parseDouble(s.substring(38, 45).trim());
				
				// MAGNITUDE
				double mag = Double.parseDouble(s.substring(47, 52).trim());
				
				if (!s.substring(45,46).equals(" "))
					throw new Exception("corrupt data at column 46");

				Hypocenter hc = new Hypocenter(new double[] {j2ksec, lon, lat, depth, mag});
				hypos.add(hc);
			}
			catch (Exception e)
			{
				System.err.println("Line " + lines + ": " + e.getMessage());
			}
		}
		rr.close();
		return hypos;
	}
	
	public static void main(String as[])
	{
		Arguments args = new Arguments(as, flags, keys);
		SQLHypocenterDataSource ds = Importer.getDataSource(args);
		process(args, new ImportH71(ds));
	}
}