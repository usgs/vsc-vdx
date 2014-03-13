package gov.usgs.vdx.data.seismic;


/**
 * 
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class WWSWaveDataSource 
{}

/*
 
implements DataSource, SelectorProvider
{
	protected WaveServer waveServer;
	
	public String getType()
	{
		return "wave";
	}

	public void initialize(Map<String, Object> params)
	{
		waveServer = (WaveServer)params.get("waveServer");
		if (waveServer == null)
		{
			String host = (String)params.get("host");
			int port = Integer.parseInt((String)params.get("port"));
			waveServer = new WaveServer(host, port);
		}
	}

	public RequestResult getData(Map<String, String> params)
	{
		String action = params.get("action");
		if (action != null && action.equals("channels"))
			return new TextResult(getChannels());
		else
		{
			double t1 = Double.parseDouble((String)params.get("st"));
			double t2 = Double.parseDouble((String)params.get("et"));
			String code = (String)params.get("channel");
			
			StringTokenizer st = new StringTokenizer(code, "_");
			String sta = st.nextToken();
			String cha = st.nextToken();
			String net = st.nextToken();
			String loc = st.nextToken();
			waveServer.connect();
			SampledWave sw = waveServer.getRawData(sta, cha, net, loc, Util.j2KToEW(t1), Util.j2KToEW(t2));
			if (sw != null)
				sw.convertToJ2K();
			waveServer.close();
			return new BinaryResult(new Wave(sw.buffer, sw.getStartTime(), sw.getSamplingRate()));
		}
	}

	public List<String> getChannels()
	{
		Menu menu = waveServer.getMenuSCNL();
		List stations = menu.getSortedItems();
		List<String> result = new ArrayList<String>(stations.size());
		for (int i = 0; i < stations.size(); i++)
		{
			MenuItem mi = (MenuItem)stations.get(i);
			result.add(mi.getSCNL() + ":" + mi.getSCNL());
		}
		return result;
	}
}
*/