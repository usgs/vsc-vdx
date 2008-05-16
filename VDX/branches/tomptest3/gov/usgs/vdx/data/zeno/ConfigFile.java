package gov.usgs.vdx.data.zeno;

import java.util.*;
import java.io.*;

/**
 *
 * @author Dan Cervelli
 */
public class ConfigFile
{
    private HashMap config;
    private boolean corrupt;
    private String name;
    
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
    
    public String get(String key)
    {
        return (String)config.get(key);
    }
    
    public HashMap getConfig()
    {
        return config;
    }
    
    public boolean isCorrupt()
    {
    	return corrupt;	
    }
    
    public String getName()
    {
    	return name;	
    }
}
