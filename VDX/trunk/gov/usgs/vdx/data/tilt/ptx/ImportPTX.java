package gov.usgs.vdx.data.tilt.ptx;

import gov.usgs.util.Util;
import gov.usgs.vdx.data.tilt.SQLTiltDataSource;
import gov.usgs.vdx.db.VDXDatabase;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Program to import Pinnacle series 5000 tiltmeter PTX files.
 *  
 * $Log: not supported by cvs2svn $
 * 
 * @author Dan Cervelli
 */
public class ImportPTX
{

	public static void main(String[] args) throws Exception
	{
		DataInputStream dis = new DataInputStream(new FileInputStream(args[0]));
		byte[] buffer = new byte[1024];
		boolean done = false;
		long lastTime = 0;
		SQLTiltDataSource dataSource = new SQLTiltDataSource();
		VDXDatabase database = VDXDatabase.getVDXDatabase("vdx.config");
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("VDXDatabase", database);
		params.put("name", args[1]);
		dataSource.initialize(params);
		while (!done)
		{
			
			try
			{
				int read = 0;
				while (read < 1024)
					read += dis.read(buffer, read, 1024 - read);
				
				PTXRecord ptx = new PTXRecord(buffer);
				if (Math.abs(lastTime - ptx.getStartTime()) > 163)
				{
					SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					df.setTimeZone(TimeZone.getTimeZone("GMT-8:00"));
					System.out.println(df.format(Util.j2KToDate(Util.ewToJ2K(ptx.getStartTime()))) + " " +Math.abs(lastTime - ptx.getStartTime()));
				}
				lastTime = ptx.getStartTime();
				
				double[][] data = ptx.getImportData();
				for (int i = 0; i < data.length; i++)
				{
					dataSource.insertData(args[2], data[i][0], data[i][1], data[i][2], 0, 1, 1, 0, 0);
				}
			}
			catch (Exception e)
			{
				done = true;
			}
		}
	}
}
