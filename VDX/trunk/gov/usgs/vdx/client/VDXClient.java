package gov.usgs.vdx.client;

import gov.usgs.net.InternetClient;
import gov.usgs.util.Log;
import gov.usgs.util.Retriable;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.BinaryDataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * TODO: proper logging.
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.5  2005/10/20 05:07:03  dcervelli
 * Added generic data type.
 *
 * Revision 1.4  2005/10/14 21:07:22  dcervelli
 * Added etilt.
 *
 * Revision 1.3  2005/09/06 21:35:54  dcervelli
 * Support for tilt data type, changed timeout to 30 seconds.
 *
 * Revision 1.2  2005/09/01 00:28:32  dcervelli
 * Fixes for changes to InternetClient.
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public class VDXClient extends InternetClient
{
	private static final int MAX_RETRIES = 3;
	protected static Map<String, String> dataTypeMap; 
	
	static
	{
		dataTypeMap = new HashMap<String, String>();
		dataTypeMap.put("hypocenters", "gov.usgs.vdx.data.hypo.HypocenterList");
		dataTypeMap.put("helicorder", "gov.usgs.vdx.data.heli.HelicorderData");
		dataTypeMap.put("rsam", "gov.usgs.vdx.data.rsam.RSAMData");
		dataTypeMap.put("wave", "gov.usgs.vdx.data.wave.Wave");
		dataTypeMap.put("gps", "gov.usgs.vdx.data.gps.GPSData");
		dataTypeMap.put("tilt", "gov.usgs.vdx.data.tilt.TiltData");
		dataTypeMap.put("etilt", "gov.usgs.vdx.data.tilt.ElectronicTiltData");
		dataTypeMap.put("generic", "gov.usgs.vdx.data.GenericDataMatrix");
	}
	
	public VDXClient(String h, int p)
	{
		super(h, p);
		logger = Log.getLogger("gov.usgs.vdx");
		setTimeout(30000);
	}
	
	public static void addDataType(String t, String c)
	{
		dataTypeMap.put(t, c);
	}
	
	protected String submitCommand(Map<String, String> params) throws IOException
	{
		if (!connected())
			connect();
		String cmd = "getdata: " + Util.mapToString(params) + "\n";
		writeString(cmd);
		
		String rs = readString();
		if (rs == null || rs.length() <= 0 || rs.indexOf(':') == -1)
			return null;
		else
			return rs;
	}
	
	public BinaryDataSet getBinaryData(final Map<String, String> params)
	{
		Retriable<BinaryDataSet> rt = new Retriable<BinaryDataSet>("VDXClient.getBinaryData()", MAX_RETRIES)
				{
					public void attemptFix()
					{
						close();
						connect();
					}
					
					public boolean attempt()
					{
						try
						{
							String rs = submitCommand(params);
							
							String rc = rs.substring(0, rs.indexOf(':'));
							String r = rs.substring(rs.indexOf(':') + 1);
							result = null;
							if (rc.equals("ok"))
							{
								System.out.println(r);
								Map<String, String> map = Util.stringToMap(r);
								if (map.get("bytes") != null)
								{
									int bytes = Integer.parseInt(map.get("bytes"));
									byte[] buffer = readBinary(bytes);
									byte[] decompBuf = Util.decompress(buffer);
									ByteBuffer bb = ByteBuffer.wrap(decompBuf);
									try
									{
										String className = dataTypeMap.get(map.get("type"));
										BinaryDataSet ds = (BinaryDataSet)Class.forName(className).newInstance();
										ds.fromBinary(bb);
	//									System.out.println(ds);
										result = ds;
									}
									catch (Exception e)
									{
										// TODO: eliminate
										e.printStackTrace();
									}
								}
								else 
								{
									System.out.println("error, expected binary");
								}
							}
							else if (rc.equals("error"))
							{
								// TODO: eliminate
								System.out.println(r);
							}
						}
						catch (Exception e)
						{
							logger.warning("VDXClient.getData() exception: " + e.getMessage());
							return false;
						}
						return true;
					}
				};
		return rt.go();
	}
	
	public List<String> getTextData(final Map<String, String> params)
	{
		Retriable<List<String>> rt = new Retriable<List<String>>("VDXClient.getTestData()", MAX_RETRIES)
		{
			public void attemptFix()
			{
				close();
				connect();
			}
			
			public boolean attempt()
			{
				try
				{
					String rs = submitCommand(params);
					
					String rc = rs.substring(0, rs.indexOf(':'));
					String r = rs.substring(rs.indexOf(':') + 1);
					result = null;
					if (rc.equals("ok"))
					{
						System.out.println(r);
						Map<String, String> map = Util.stringToMap(r);
						if (map.get("lines") != null)
						{
							int lines = Integer.parseInt(map.get("lines"));
							List<String> list = new ArrayList<String>();
							for (int i = 0; i < lines; i++)
								list.add(readString());
							result = list;
						}
						else 
						{
							System.out.println("error, expected text");
						}
					}
					else if (rc.equals("error"))
					{
						// TODO: eliminate
						System.out.println(r);
					}
				}
				catch (Exception e)
				{
					logger.warning("VDXClient.getData() exception: " + e.getMessage());
					return false;
				}
				return true;
			}
		};
		return rt.go();
	}
	
	public static void main(String[] args)
	{
//		VDXClient client = new VDXClient(args[0], Integer.parseInt(args[1]));
	}
}
