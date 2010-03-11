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
 * Customized internet client, can connect to specified host-port and read raw data.
 * Can issue command and get command processing result. 
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
		dataTypeMap.put("genericfixed", "gov.usgs.vdx.data.GenericDataMatrix");
		dataTypeMap.put("genericvariable", "gov.usgs.vdx.data.GenericDataMatrix");
		dataTypeMap.put("gps", "gov.usgs.vdx.data.gps.GPSData");
		dataTypeMap.put("helicorder", "gov.usgs.vdx.data.heli.HelicorderData");
		dataTypeMap.put("hypocenters", "gov.usgs.vdx.data.hypo.HypocenterList");
		dataTypeMap.put("rsam", "gov.usgs.vdx.data.rsam.RSAMData");
		dataTypeMap.put("ewrsam", "gov.usgs.vdx.data.rsam.EWRSAMData");
		dataTypeMap.put("tilt", "gov.usgs.vdx.data.tilt.TiltData");
		dataTypeMap.put("wave", "gov.usgs.vdx.data.wave.Wave");
	}
	
	/**
	 * Constructor
	 * @param h host to connect
	 * @param p port to connect
	 */
	public VDXClient(String h, int p)
	{
		super(h, p);
		logger = Log.getLogger("gov.usgs.vdx");
		setTimeout(30000);
	}
	
	/**
	 * Adds resulting data type to internal map
	 * @param t type name
	 * @param c class name to process this type's data
	 */
	public static void addDataType(String t, String c)
	{
		dataTypeMap.put(t, c);
	}
	
	/**
	 * Issue command to server. 
	 * Command is map of parameters - param_name - param_value pairs.
	 * @return Command result got from server as string
	 */
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
	
	/**
	 * Issue command to server and get binary data response. 
	 * Command is map of parameters - param_name - param_value pairs.
	 * @return Command result got from server and parsed in BinaryDataSet
	 */
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
										System.out.println(":VDX client got type " + map.get("type"));
										String className = dataTypeMap.get(map.get("type"));
										BinaryDataSet ds = (BinaryDataSet)Class.forName(className).newInstance();
										ds.fromBinary(bb);
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
	
	/**
	 * Issue command to server and get text data response. 
	 * Command is map of parameters - param_name - param_value pairs.
	 * @return Command result got from server and parsed List<String>
	 */
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
					logger.info("VDXClient.getData(): params = " + params);
					String rs = submitCommand(params);
					
					String rc = rs.substring(0, rs.indexOf(':'));
					String r = rs.substring(rs.indexOf(':') + 1);
					result = null;
					logger.info("VDXClient.getData(): rc = " + rc);
					logger.info("VDXClient.getData(): r = " + r);
				
					if (rc.equals("ok"))
					{
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
							logger.warning("VDXClient.getData(): error, expected text");
						}
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
