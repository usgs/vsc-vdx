package gov.usgs.volcanoes.vdx.data.wave;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.core.data.Wave;
import gov.usgs.volcanoes.core.legacy.ew.Menu;
import gov.usgs.volcanoes.core.legacy.ew.MenuItem;
import gov.usgs.volcanoes.core.legacy.ew.WaveServer;
import gov.usgs.volcanoes.core.time.Time;
import gov.usgs.volcanoes.core.util.StringUtils;
import gov.usgs.volcanoes.vdx.data.DataSource;
import gov.usgs.volcanoes.vdx.server.BinaryResult;
import gov.usgs.volcanoes.vdx.server.RequestResult;
import gov.usgs.volcanoes.vdx.server.TextResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 
 * Earthworm WSV data source for VDX
 * 
 * @author Dan Cervelli
 */
public class WaveServerVSource implements DataSource
{
	private String host;
	private int port;
	private int timeout;
	private int maxrows = 0;
	
	
	private WaveServer waveServer;

	public WaveServerVSource()
	{
		maxrows = 0;
	}
	
	public String getType()
	{
		return "wave";
	}
	
	public int getMaxRows(){
		return maxrows;
	}
	
	public void initialize(ConfigFile params)
	{
		host = params.getString("host");
		port = Integer.parseInt(params.getString("port"));
		timeout = Integer.parseInt(params.getString("timeout"));
		maxrows	= StringUtils.stringToInt(params.getString("maxrows"), 0);
        waveServer = new WaveServer(host, port);
        waveServer.setTimeout(timeout);
	}
	
	public RequestResult getData(Map<String, String> params)
	{
		String action = params.get("action");
		if (action != null && action.equals("selectors"))
		{
			Menu menu = waveServer.getMenuSCNL();
			List<MenuItem> stations = menu.getSortedItems();
			List<String> result = new ArrayList<String>(stations.size());
			for (int i = 0; i < stations.size(); i++)
			{
				MenuItem mi = (MenuItem)stations.get(i);
				result.add(mi.getSCN("_") + ":-999:-999:" + mi.getSCN("_") + ":" + mi.getSCN("_"));
			}
			return new TextResult(result);
		}
		else
		{
			// TODO: validate
			double t1 = Double.parseDouble(params.get("st"));
			double t2 = Double.parseDouble(params.get("et"));
			String code = params.get("selector");
			if (code == null)
				return null;
			
			String[] ss = code.split("_");
			if (ss.length < 3)
				return null;
			
			String sta = ss[0];
			String cha = ss[1];
			String net = ss[2];
			String loc = null;
			if (ss.length >= 4)
				loc = ss[3];
			waveServer.connect();
			Wave sw = waveServer.getRawData(sta, cha, net, loc, Time.j2kToEw(t1), Time.j2kToEw(t2));
			if (sw != null)
				sw.convertToJ2K();
			waveServer.close();
			return new BinaryResult(sw);
		}
	}
}
