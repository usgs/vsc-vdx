package gov.usgs.vdx.in.conn;

import gov.usgs.util.ConfigFile;
import gov.usgs.vdx.in.hw.Device;
/**
 * Interface to represent connection to a device
 * 
 * @author Loren Antolik
 */
public interface Connection {
	public void initialize (ConfigFile params) throws Exception;
	public void connect () throws Exception;
	public void disconnect ();
	public void writeString (String dataRequest) throws Exception;
	public String readString (Device device) throws Exception;
	public String toString();
}