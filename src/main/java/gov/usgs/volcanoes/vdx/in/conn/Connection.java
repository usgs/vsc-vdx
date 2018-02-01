package gov.usgs.volcanoes.vdx.in.conn;

import gov.usgs.volcanoes.core.configfile.ConfigFile;
import gov.usgs.volcanoes.vdx.in.hw.Device;
/**
 * Interface to represent connection to a device
 * 
 * @author Loren Antolik
 */
public interface Connection {
	public void initialize (ConfigFile params) throws Exception;
	public void connect () throws Exception;
	public void disconnect();
	public void writeString (String dataRequest) throws Exception;
	public String readString (Device device) throws Exception;
	public String toString();
	public String getMsgQueue();
	public void emptyMsgQueue();
	public boolean isOpen();
}
