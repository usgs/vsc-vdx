package gov.usgs.vdx.data.tilt.ptx;

import gov.usgs.vdx.data.tilt.SQLElectronicTiltDataSource;
import gov.usgs.vdx.db.VDXDatabase;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Program to import Pinnacle series 5000 tiltmeter PTX files.
 *  
 * $Log: not supported by cvs2svn $
 * Revision 1.2  2005/09/21 20:02:08  dcervelli
 * Changed argument order.
 *
 * 
 * @author Dan Cervelli
 */
public class ImportPTX
{

	public static void main(String[] args) throws Exception
	{
		SQLElectronicTiltDataSource dataSource = new SQLElectronicTiltDataSource();
		VDXDatabase database = VDXDatabase.getVDXDatabase("vdx.config");
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("VDXDatabase", database);
		params.put("name", args[0]);
		String channel = args[1];
		dataSource.initialize(params);
		
		byte[] buffer = new byte[1024];
		boolean done = false;
//		long lastTime = 0;
		for (int j = 2; j < args.length; j++)
		{
			System.out.println("file: " + args[j]);
			DataInputStream dis = new DataInputStream(new FileInputStream(args[j]));
			while (!done)
			{
							
				try
				{
					int read = 0;
					while (read < 1024)
						read += dis.read(buffer, read, 1024 - read);
					
					PTXRecord ptx = new PTXRecord(buffer);
					double[][] data = ptx.getImportData();
					for (int i = 0; i < data.length; i++)
					{
						dataSource.insertData(channel, data[i][0], data[i][1], data[i][2], data[i][3], data[i][4], 0, 1, 1, 0, 0, 1, 0, 1, 0);
					}
				}
				catch (Exception e)
				{
					done = true;
				}
			}
			dis.close();
		}
	}
}
