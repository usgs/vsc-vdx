package gov.usgs.vdx.data.zeno;

import java.util.*;
import java.io.*;

/**
 * Represent configuration file. 
 * Read disk file of 'param = value' structure and keep map of parameters.
 * '#' is a comment line mark.
 * 
 * @author Dan Cervelli
 */
public class ConfigFile
{
    private HashMap config;
    private boolean corrupt;
    private String name;
    
    /**
     * Constructor
     * @param fn config file name
     */
    public ConfigFile(String fn)
    {
    	if (fn.endsWith(".config"))
    		name = fn.substring(0, fn.indexOf(".config"));
    	else
    		name = fn;
        config = new HashMap();
        corrupt = false;
        readConfigFile(fn);
    }
 
    /**
     * Reads configuration file and initialize this object
     * @param fn
     */
    public void readConfigFile(String fn)
    {
        try
        {
            BufferedReader in = new BufferedReader(new FileReader(fn));
            String s;
            while ((s = in.readLine()) != null)
            {
                s = s.trim();
                // skip whitespace and comments
                if (s.length() != 0 && !s.startsWith("#"))
                {
                    String key = s.substring(0, s.indexOf('='));
                    String val = s.substring(s.indexOf('=') + 1);
                    config.put(key, val);
                }
            }
            in.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            corrupt = true;
        }
    }
 
    /**
     * Get parameter value
     * @param key param name
     */
    public String get(String key)
    {
        return (String)config.get(key);
    }
 
    /**
     * Get whole map of parameters
     */
    public HashMap getConfig()
    {
        return config;
    }
    
    /**
     * Get flag if disk configuration file was parsed successfully
      */
    public boolean isCorrupt()
    {
    	return corrupt;	
    }
    
    /**
     * Get configuration name (part of disk file name without '.config')
      */
    public String getName()
    {
    	return name;	
    }
}
