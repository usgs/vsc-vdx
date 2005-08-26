package gov.usgs.vdx.data.gps;

import gov.usgs.util.CodeTimer;
import gov.usgs.util.ConfigFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class ImportStacov
{	
//	private static SQLGPSDataManager gdm;

	public static void initialize(String d, String u)
	{
//		SQLGPSDataManager.initialize(d, u);
//		gdm = (SQLGPSDataManager)SQLGPSDataManager.getInstance();		
	}
	
	public void importFile(String fn, boolean useFileDate)
	{
		try
		{
			System.out.println("working on file '" + fn + "'");
			SimpleDateFormat dateIn;
			if (useFileDate)
				dateIn = new SimpleDateFormat("yyMMddHHmm");
			else
				dateIn = new SimpleDateFormat("yyMMMdd");
			SimpleDateFormat dateOut = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			dateIn.setTimeZone(TimeZone.getTimeZone("GMT"));
			dateOut.setTimeZone(TimeZone.getTimeZone("GMT"));
			BufferedReader in = new BufferedReader(new FileReader(fn));
			String s = in.readLine();
			int numParams = Integer.parseInt(s.substring(0, 5).trim());
			Station[] stations = new Station[numParams / 3];
			Date fileDate = null;
			if (useFileDate)
			{
				int idx = fn.lastIndexOf('\\');
				fileDate = dateIn.parse(fn.substring(idx + 2, idx + 12));
			}
			else
			{
				fileDate = dateIn.parse(s.substring(20, 27));
				fileDate.setTime(fileDate.getTime() + 12 * 60 * 60 * 1000);
			}
			double j2ksec = (((double)fileDate.getTime() / (double)1000) - 946728000);
//			gdm.deleteData(j2ksec);
			CodeTimer ct = new CodeTimer("read stations");
			for (int i = 0; i < numParams / 3; i++)
			{
				String sx = in.readLine();
				String sy = in.readLine();
				String sz = in.readLine();
				Station st = new Station();
				st.j2ksec = j2ksec;
				st.code = sx.substring(7, 11).trim();
				//if (st.code.length() == 3)
				//	st.code = st.code + "_";
//				st.sid = gdm.getStationID(st.code);
				st.x = Double.parseDouble(sx.substring(25, 47).trim());
				st.sx = Double.parseDouble(sx.substring(53, 74).trim());
				st.y = Double.parseDouble(sy.substring(25, 47).trim());
				st.sy = Double.parseDouble(sy.substring(53, 74).trim());
				st.z = Double.parseDouble(sz.substring(25, 47).trim());
				st.sz = Double.parseDouble(sz.substring(53, 74).trim());
				stations[i] = st;
//public void addData(int sid, double j2ksec, String time, double x, double y, double z, double sx, double sy, double sz)				
//				gdm.addData(st.sid, j2ksec, dateOut.format(fileDate), st.x, st.y, st.z, st.sx, st.sy, st.sz);
			}
			ct.stop();
			ct = new CodeTimer("read cov");
			boolean done = false;
			while (!done)
			{
				try
				{
					String sc = in.readLine();
					if (sc != null && sc.length() >= 2)
					{
						int p1 = Integer.parseInt(sc.substring(0, 5).trim()) - 1;
						int p2 = Integer.parseInt(sc.substring(5, 11).trim()) - 1;
						double data = Double.parseDouble(sc.substring(13).trim());
						Station s1 = stations[p1 / 3];
						Station s2 = stations[p2 / 3];
	//public void addCovData(double j2ksec, int sid1, int sid2, int c1, int c2, double data)					
//						gdm.addCovData(j2ksec, s1.sid, s2.sid, p1 % 3, p2 % 3, data);
					}
					else
						done = true;
				}
				catch (NumberFormatException e)
				{
					done = true;	
				}
			}
			in.close();
			ct.stop();
//			gdm.addFrame(j2ksec, 0, 0, 0);
		}
		catch (Exception e)
		{
			e.printStackTrace();	
		}
				
	}

	public static void main(String args[])
	{
		ImportStacov is = new ImportStacov();
		is.importFile(args[0], false);
		System.exit(1);
		
		if (args.length == 0)
		{
			System.out.println("specify an input file or directory");	
			System.exit(1);
		}
		String d = null;
		String u = null;
		boolean ufd = false;
		if (args.length == 3)
		{
			d = args[1];
			u = args[2];	
		}
		else if (new File("ImportStacov.config").exists())
		{
			ConfigFile cf = new ConfigFile("ImportStacov.config");
			d = cf.getString("driver");
			u = cf.getString("url");
			if (cf.getString("useFileDate") != null && cf.getString("useFileDate").equals("true"))
				ufd = true;
		}
		else
		{
			System.out.println("you must specify a database driver and url either on the command line after the input file: [input file] [driver] [url]");
			System.out.println("or in a file called 'ImportStacov.config' with lines like:");
			System.out.println();
			System.out.println("driver=driver.class.name");
			System.out.println("url=jdbc://url");
			System.out.println("# useFileDate gets the date from the file name, not the stacov file.");
			System.out.println("# This is for processing more frequent than daily solutions.");
			System.out.println("useFileDate=false");
			
			System.exit(1);
		}
		
		initialize(d, u);
//		ImportStacov is = new ImportStacov();
		File f = new File(args[0]);
		if (f.isDirectory())
		{
			File files[] = f.listFiles();
			for (int i = 0; i < files.length; i++)
			{
				if (files[i].getName().endsWith(".stacov"))
					is.importFile(files[i].getPath(), ufd);
			}
		}
		else
			is.importFile(args[0], false);	
	}	
	
	class Station
	{
		double j2ksec;
		String code;
		int sid;
		double x, y, z;
		double sx, sy, sz;
		double cxy, cxz, cyz;
	}
	
	class Covariance
	{
		double j2ksec;
		int sid1, sid2;
		double xx, xy, xz;
		double yx, yy, yz;
		double zx, zy, zz;
	}
	
}
