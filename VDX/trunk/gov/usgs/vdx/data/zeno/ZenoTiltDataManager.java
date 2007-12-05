package gov.usgs.vdx.data.zeno;

import java.sql.*;

public class ZenoTiltDataManager extends SQLTiltDataManager
{
	/** The singleton data manager.
	 */
    protected static ZenoTiltDataManager tiltDataManager;
    
	/** The only constructor.  Protected because this class is a singleton.
	 * SQLTiltDataManagers should be acquired through getInstance().
	 * @param d the database driver
	 * @param u the database url
	 */
    protected ZenoTiltDataManager(String d, String u)
    {
        super(d, u);
    }
    
	/** Initialization function called by independent non-Valve programs.
	 * Sets the driver and URL manually.
	 * @param d the driver
	 * @param u the url
	 */
    public static void initialize(String d, String u)
    {
        if (tiltDataManager == null)
            tiltDataManager = new ZenoTiltDataManager(d, u);
    }
	
	/** Gets the singleton SQLTiltDataManager.
	 * @return the SQLTiltDataManager
	 */
    public static DataManager getInstance()
    {
        return tiltDataManager;
    }
	
	/** Gets the last clock synchronization time.
	 * @param code the station code
	 * @return the last synchronization time (j2ksec)
	 */
	public double getLastSyncTime(String code)
	{
		try
		{
			ResultSet rs = getStatement().executeQuery("SELECT lastsync FROM stations WHERE code='" + code + "'");
			rs.next();
			return rs.getDouble("lastsynctime");
		}
		catch (Exception e)
		{
			e.printStackTrace();	
		}
		return Double.NaN;
	}
	
	/** Sets the last clock synchronization time.
	 * @param code the station code
	 * @param j2ksec the synchronization time (j2ksec)
	 */
	public void setLastSyncTime(String code, double j2ksec)
	{
		try
		{
			getStatement().execute("UPDATE stations SET lastsync=" + j2ksec + " WHERE code='" + code + "'");
		}
		catch (Exception e)
		{
			e.printStackTrace();	
		}
	}
}