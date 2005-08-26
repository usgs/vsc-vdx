package gov.usgs.vdx.client;

import gov.usgs.net.InternetClient;
import gov.usgs.util.Retriable;
import gov.usgs.util.RetryManager;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.BinaryDataSet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class VDXClient extends InternetClient
{
	protected static Map<String, String> dataTypeMap; 
	
	static
	{
		dataTypeMap = new HashMap<String, String>();
		dataTypeMap.put("hypocenters", "gov.usgs.vdx.data.hypo.HypocenterList");
		dataTypeMap.put("helicorder", "gov.usgs.vdx.data.heli.HelicorderData");
		dataTypeMap.put("rsam", "gov.usgs.vdx.data.rsam.RSAMData");
		dataTypeMap.put("wave", "gov.usgs.vdx.data.wave.Wave");
		dataTypeMap.put("gps", "gov.usgs.vdx.data.gps.GPSData");
	}
	
	public VDXClient(String h, int p)
	{
		super(h, p);
	}
	
	public static void addDataType(String t, String c)
	{
		dataTypeMap.put(t, c);
	}
	
	public Object getData(final Map<String, String> params)
	{
		RetryManager rm = new RetryManager();
		Object finalResult = rm.attempt(new Retriable()
				{
					public void attemptFix()
					{
						close();
					}
					
					public boolean attempt()
					{
						String cmd = "getdata: " + Util.mapToString(params) + "\n";
						writeString(cmd);
						
						String rs = readString();
						if (rs == null || rs.length() <= 0 || rs.indexOf(':') == -1)
							return false;
						
						String rc = rs.substring(0, rs.indexOf(':'));
						String r = rs.substring(rs.indexOf(':') + 1);
						Object data = null;
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
									data = ds;
								}
								catch (Exception e)
								{
									e.printStackTrace();
								}
							}
							else if (map.get("lines") != null)
							{
								int lines = Integer.parseInt(map.get("lines"));
								List<String> list = new ArrayList<String>();
								for (int i = 0; i < lines; i++)
									list.add(readString());
				
								data = list;
							}
						}
						else if (rc.equals("error"))
						{
							System.out.println(r);
						}
							
						result = data;
						return true;
					}
				});
		return finalResult;
	}
	
	public static void main(String[] args)
	{
		VDXClient client = new VDXClient("localhost", 16050);
		
		HashMap<String, String> p = new HashMap<String, String>();
		//get: source=ak_eqs; st=1.578528E8; et=1.734912E8; west=-179; east=-140; south=50; north=80;
		//minDepth=-1000; maxDepth=1000; minMag=-100; maxMag=100;
		p.put("source", "ak_eqs");
		p.put("st", "1.578528E8");
		p.put("et", "1.778528E8");
		p.put("west", "-179");
		p.put("east", "-40");
		p.put("south", "50");
		p.put("north", "80");
		p.put("minDepth", "-1000");
		p.put("maxDepth", "1000");
		p.put("minMag", "-100");
		p.put("maxMag", "100");
		client.getData(p);
		
		
		/*
		HashMap<String, String> p = new HashMap<String, String>();
		//get: source=ak_eqs; st=1.578528E8; et=1.734912E8; west=-179; east=-140; south=50; north=80;
		//minDepth=-1000; maxDepth=1000; minMag=-100; maxMag=100;
		p.put("source", "ak_waves");
		p.put("st", "1.773756E8");
		p.put("et", "1.773792E8");
		p.put("selector", "GAEA_SHZ_AK_--");
		client.getData(p);
		*/
	}
}
