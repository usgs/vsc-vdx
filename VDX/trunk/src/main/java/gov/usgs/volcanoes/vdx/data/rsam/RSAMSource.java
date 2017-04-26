package gov.usgs.volcanoes.vdx.data.rsam;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import gov.usgs.math.DownsamplingType;
import gov.usgs.plot.data.RSAMData;
import gov.usgs.volcanoes.core.util.UtilException;
import gov.usgs.volcanoes.vdx.data.VDXSource;
import gov.usgs.volcanoes.vdx.server.BinaryResult;
import gov.usgs.volcanoes.vdx.server.RequestResult;
import gov.usgs.volcanoes.vdx.server.TextResult;
import gov.usgs.volcanoes.winston.Channel;

/**
 * 
 * @author Dan Cervelli
 */
public class RSAMSource extends VDXSource {
	
	public String getType() {
		return "rsam";
	}
	
	public void disconnect() {
		defaultDisconnect();
	}
	
	public RequestResult getData(Map<String, String> params) {
		
		String action = params.get("action");
		
		if (action.equals("channels")) {
			List<Channel> chs	= channels.getChannels();
			List<String> result	= new ArrayList<String>();
			for (Channel ch : chs)
				result.add(ch.toVDXString());
			return new TextResult(result);
			
		} else if (action.equals("ratdata")){
			String cids		= params.get("ch");
			double st		= Double.parseDouble(params.get("st"));
			double et		= Double.parseDouble(params.get("et"));
			DownsamplingType ds = DownsamplingType.fromString(params.get("ds"));
			int dsInt		= Integer.parseInt(params.get("dsInt")); 
			RSAMData data = null;
			try{
				data = getRatSAMData(cids, st, et, getMaxRows(), ds, dsInt);
			} catch (UtilException e){
				return getErrorResult(e.getMessage());
			}
			if (data != null) {
				return new BinaryResult(data);
			}
			
		} else if (action.equals("data") || action == null){
			int cid			= Integer.parseInt(params.get("ch"));
			double st		= Double.parseDouble(params.get("st"));
			double et		= Double.parseDouble(params.get("et"));
			DownsamplingType ds = DownsamplingType.fromString(params.get("ds"));
			int dsInt		= Integer.parseInt(params.get("dsInt")); 
			
			String code		= channels.getChannelCode(cid);
			RSAMData data = null;
			try{
				data = getData(code, st, et, getMaxRows(), ds, dsInt);
			} catch (UtilException e){
				return getErrorResult(e.getMessage());
			}
			if (data != null) {
				return new BinaryResult(data);
			}
		}
		
		return null;
	}
	
	protected RSAMData getData(String code, double st, double et, int maxrows, DownsamplingType ds, int dsInt) throws UtilException {
		return data.getRSAMData(code, st, et, maxrows, ds, dsInt);
	}
	
	protected RSAMData getRatSAMData(String code, double st, double et, int maxrows, DownsamplingType ds, int dsInt) throws UtilException {
		RSAMData result1	= null;
		RSAMData result2	= null;
		
		String[] codes		= code.split(",");
		String code1		= channels.getChannelCode(Integer.valueOf(codes[0]));
		String code2		= channels.getChannelCode(Integer.valueOf(codes[1]));
		result1				= getData(code1, st, et, maxrows, ds, dsInt);
		result2				= getData(code2, st, et, maxrows, ds, dsInt);
		
		return result1.getRatSAM(result2);
	}
}
