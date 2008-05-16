package gov.usgs.vdx.data.zeno;

/**
 * This class is mainly used as a tag to specify that a class is a DataManager
 * that the DataManagerManager can recognize.
 *
 * $Log: not supported by cvs2svn $
 *
 * @@author  Dan Cervelli
 * @@version 2.00
 */
public class DataManager
{
	/** Gets an instance of this DataManager. This is the preferred way to get 
	 * an instance of a particular DataManager.  This way the implementation 
	 * details (specifically the fact that all of the provided DataManagers
	 * are singletons) are hidden from the user.
	 *
	 * @@return a DataManager object
	 */
    public static DataManager getInstance()
    {
        return null;
    }
    
	/** Initializes the DataManager.  This is called by the DataManagerManager
	 * and is used for initialization -- setting database parameters, reading
	 * configuration files, etc.
	 */
    public static void initialize() {}
}