package gov.usgs.vdx.data.gps;

import gov.usgs.util.ResourceReader;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class ImportSum
{
	private SQLGPSDataSource dataSource;

	public ImportSum()
	{
		dataSource = new SQLGPSDataSource();
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("vdx.host", "localhost");
		params.put("vdx.name", "v");
		params.put("vdx.databaseName", "msh");
		dataSource.initialize(params);
	}
	
	public void importSum(String fn)
	{
		ResourceReader rr = ResourceReader.getResourceReader(fn);
		if (rr == null)
			return;
		String[] ss = rr.nextLine().split("\t");
		String bm = ss[1].toUpperCase();
		ss = rr.nextLine().split("\t");
		double dlat = Double.parseDouble(ss[1].trim());
		double sign = 1;
		if (dlat < 0)
			sign = -1;
		dlat = Math.abs(dlat);
		double lat = dlat + Double.parseDouble(ss[2].trim()) / 60 + Double.parseDouble(ss[3].trim()) / 3600;
		lat *= sign;
		
		ss = rr.nextLine().split("\t");
		double dlon = Double.parseDouble(ss[1].trim());
		sign = 1;
		if (dlon < 0)
			sign = -1;
		dlon = Math.abs(dlon);
		double lon = dlon + Double.parseDouble(ss[2].trim()) / 60 + Double.parseDouble(ss[3].trim()) / 3600;
		lon *= sign;
		
		ss = rr.nextLine().split("\t");
		double height = Double.parseDouble(ss[1].trim());
		
		dataSource.insertBenchmark(bm, lon, lat, height, 1);
		System.out.printf("%s %f %f %f\n", bm, lon, lat, height);
	}
	
	public static void main(String[] args)
	{
		ImportSum is = new ImportSum();
		for (int i = 0; i < args.length; i++)
			is.importSum(args[i]);
	}
}
