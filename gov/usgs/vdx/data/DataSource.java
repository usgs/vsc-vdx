package gov.usgs.vdx.data;

import gov.usgs.util.ConfigFile;
import gov.usgs.vdx.server.RequestResult;

import java.util.Map;

/**
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * @author Dan Cervelli
 */
public interface DataSource 
{
	public String getType();
	public void initialize(ConfigFile params);
	public RequestResult getData(Map<String, String> params);
}
