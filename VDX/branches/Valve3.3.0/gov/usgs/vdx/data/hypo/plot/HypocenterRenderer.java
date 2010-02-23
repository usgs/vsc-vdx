package gov.usgs.vdx.data.hypo.plot;

import gov.usgs.plot.Jet;
import gov.usgs.plot.Renderer;
import gov.usgs.plot.SmartTick;
import gov.usgs.plot.Spectrum;
import gov.usgs.plot.Transformer;
import gov.usgs.vdx.data.hypo.Hypocenter;
import gov.usgs.vdx.data.hypo.HypocenterList;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Stroke;
import java.awt.geom.AffineTransform;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Line2D;
import java.awt.geom.Rectangle2D;
import java.text.NumberFormat;
import java.util.List;

/**
 * HypocenterRenderer is a class that render a list of hypocenters in a variety
 * of different ways.
 *
 * @author Dan Cervelli 
 */
public class HypocenterRenderer implements Renderer
{
	/**
	 * Enumeration to define scale type
	 */
	public enum Axes
	{
		MAP_VIEW, LON_DEPTH, LAT_DEPTH, DEPTH_TIME, TRIPLE_VIEW;
		
		public static Axes fromString(String c)
		{
			if (c == null)
				return null;
			switch(c.charAt(0))
			{
				case 'M':
					return MAP_VIEW;
				case 'E':
					return LON_DEPTH;
				case 'N':
					return LAT_DEPTH;
				case 'D':
					return DEPTH_TIME;
				case '3':
					return TRIPLE_VIEW;
				default:
					return null;
			}
		}
	}
	
	/**
	 * Enumeration to define coloring mode
	 */
	public enum ColorOption
	{
		DEPTH, TIME, MONOCHROME;
		
		public static ColorOption fromString(String c)
		{
			if (c == null)
				return null;
			switch (c.charAt(0))
			{
				case 'D':
					return DEPTH; //Coloring by depth
				case 'T':
					return TIME;  //Coloring by time
				case 'M':
					return MONOCHROME;
				default:
					return null;
			}
		}
		
		public static ColorOption chooseAuto(Axes ax)
		{
			if (ax == null)
				return null;
			
			switch (ax)
			{
				case MAP_VIEW:
					return DEPTH;
				case LON_DEPTH:
				case LAT_DEPTH:
					return TIME;
				default:
					return MONOCHROME;
			}
		}
	}
	
    private HypocenterList data;
    private Transformer transformer;
    private Axes axes;
    private static NumberFormat numberFormat;
    private ColorOption colorOpt;
    private double minTime;
    private double maxTime;
    private static final Spectrum spectrum = Jet.getInstance();

    private Renderer colorScaleRenderer;
    private Renderer magnitudeScaleRenderer;
    
    /** 
     * The shapes that used to render different magnitude earthquakes
	 */
    public static Ellipse2D.Float[] circles = new Ellipse2D.Float[]
    {
        new Ellipse2D.Float(-1.5f, -1.5f, 3, 3),
        new Ellipse2D.Float(-3, -3, 6, 6),
        new Ellipse2D.Float(-5.5f, -5.5f, 11, 11),
        new Ellipse2D.Float(-9, -9, 18, 18),
        new Ellipse2D.Float(-13.5f, -13.5f, 27, 27),
        new Ellipse2D.Float(-19, -19, 38, 38),
        new Ellipse2D.Float(-25.5f, -25.5f, 51, 51),
        new Ellipse2D.Float(-34f, -34f, 68, 68),
    };
    
    public static final int[] circleScaleOffset = new int[] {1, 8, 17, 28, 44, 64, 90, 120};
    
    // set these externally
	/** 
	 * The colors for different depths
	 */
    public static Color[] colors = new Color[] {Color.BLACK, Color.GREEN, Color.RED, new Color(1.0f, 0.91f, 0.0f), Color.BLUE, new Color(0.8f, 0f, 1f), Color.BLACK};
	
	/** 
	 * The depth intervals
	 */
    public static double[] depths = new double[] {10000, 0, -5, -10, -20, -40, -70, -10000};
    
	/** Constructor for HypocenterRenderer that gets the data, the coordinate
	 * transformer and the view type.  The different views are:<br>
	 * M: map view (axes=lon/lat)<br>
	 * E or A: east view (axes=lon/depth)<br>
	 * N: north view (axes=lat/depth)<br>
	 * T: time view (axes=time/lon)<br>
	 * @param d the earthquake data
	 * @param fr the coordinate transformer (usually a FrameRenderer)
	 * @param v the view type 
	 */
	public HypocenterRenderer(HypocenterList d, Transformer fr, Axes v)
    {
        data = d;
        transformer = fr;
        axes = v;
        if (numberFormat == null)
        {
            numberFormat = NumberFormat.getInstance();    
            numberFormat.setMaximumFractionDigits(0);
        }
        colorOpt = ColorOption.chooseAuto(v);
    }
    
	/** 
	 * Sets the renderer to operate in color-time mode where time is 
	 * represented in color instead of depth.
	 * @param min the minimum time (j2ksec) for the coolest color
	 * @param max the maximum time (j2ksec) for the warmest color
	 */
    public void setColorTime(double min, double max)
    {
//    	if (axes == Axes.TRIPLE_VIEW)
//    		colorOpt = "T";
    	colorOpt = ColorOption.TIME;
        minTime = min;
        maxTime = max;
    }
 
    /**
     * Sets monochrome color mode
     */
    public void setMonochrome()
    {
    	colorOpt = ColorOption.MONOCHROME;
    }
    
    /**
     * Sets depth color mode
     */
    public void setColorDepth()
    {
    	colorOpt = ColorOption.DEPTH;
    }
    
    /**
     * Sets color mode
     * @param c Color option to set
     */
    public void setColorOption(ColorOption c)
    {
    	colorOpt = c;
    }
    
    /**
     * Create renderer to draw hypocenter's magnitude
     * @param xStart hypocenter's X pixel coordinate
     * @param yStart hypocenter's Y pixel coordinate
     */
    public void createMagnitudeScaleRenderer(final double xStart, final double yStart)
    {
    	magnitudeScaleRenderer = new Renderer()
		    	{
		    		public void render(Graphics2D g)
		    		{
		    			g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		                g.setColor(Color.BLACK);
		                g.setStroke(new BasicStroke(2.0f));
		
		                g.drawString("Magnitude", (float)xStart - 4, (float)yStart - 40);
		                g.setFont(new Font("Arial", Font.PLAIN, 10));
		                
		    			for (int i = 0; i < HypocenterRenderer.circles.length - 2; i++)
		                {
		                    g.translate(xStart - HypocenterRenderer.circles[i].x + 8, HypocenterRenderer.circles[i].y + yStart + circleScaleOffset[i]);
		                    g.setPaint(Color.WHITE);
		                    g.fill(HypocenterRenderer.circles[i]);
		                    g.setPaint(Color.BLACK);
		                    g.draw(HypocenterRenderer.circles[i]);
		                    g.translate(-(xStart - HypocenterRenderer.circles[i].x + 8), -(HypocenterRenderer.circles[i].y + yStart + circleScaleOffset[i]));
		                    g.drawString("" + i, (float)xStart, (float)yStart + circleScaleOffset[i]);
		                }	
		    		}
		    	};
    }
    
    /**
     * Create renderer to draw hypocenter in current color mode and scale type
     * @param xStart hypocenter's X pixel coordinate
     * @param yStart hypocenter's Y pixel coordinate
     */
    public void createColorScaleRenderer(final double xStart, final double yStart)
    {
    	colorScaleRenderer = new Renderer()
		    	{
		    		public void render(Graphics2D g)
		    		{
		    			g.setStroke(new BasicStroke(1.0f));
		                g.setFont(new Font("Arial", Font.PLAIN, 10));
		    			if (colorOpt == ColorOption.DEPTH || axes == Axes.TRIPLE_VIEW)
		                {
		    				Rectangle2D.Double rect = new Rectangle2D.Double();
		                	float dy = 0;
//		                	if (axes == Axes.TRIPLE_VIEW)
//		                		dy = -60;
		                	double yoff = 0;
		                	double ty = (depths.length - 3) * 13;
		                    for (int i = 1; i < depths.length - 2; i++)
		                    {
		                        rect.setRect(xStart, yStart - ty + yoff + dy, 10, 13);
		                        g.drawString("" + Math.round(-depths[i]), (float)xStart + 13, (float)(yStart - ty + 4 + dy + yoff));
		                        g.setPaint(colors[i]);
		                        g.fill(rect);
		                        g.setPaint(Color.BLACK);
		                        g.draw(rect);
		                        yoff += 13;
		                    }
		                    g.drawString("" + Math.round(-depths[depths.length - 2]), (float)xStart + 13, (float)(yStart - ty + yoff + 4 + dy));
		                    //g.drawString("Depth (km)", (float)xStart + 240, (float)yStart + 15);
		                    g.drawString("Depth (km)", (float)xStart, (float)(yStart - ty - 9 + dy));
		                    
		                }
		    			
		                if (colorOpt == ColorOption.TIME || axes == Axes.TRIPLE_VIEW)
		                {
//		                	if (axes == Axes.TRIPLE_VIEW)
//		                		yoff += 45;
//		                	spectrum.renderScale(g, xoff - 15, yoff, 10, 80, true, true);
		                	spectrum.renderScale(g, xStart, yStart - 80, 10, 80, true, true);
		                    Object[] t = SmartTick.autoTimeTick(minTime, maxTime, 6);
		                    double[] ticks = (double[])t[0];
		                    String[] labels = (String[])t[1];
		                    double dt = maxTime - minTime;
		                    Line2D.Double line = new Line2D.Double();
		                    for (int i = 0; i < ticks.length; i++)
		                    {
		                    	double yl = ((ticks[i] - minTime) / dt) * 80;
		                        line.setLine(xStart + 10, yStart - 80 + yl, xStart + 13, yStart - 80 + yl);
		                        g.draw(line);
		                        g.drawString(labels[i], (float)xStart + 16, (float)(yl + yStart - 80 + 3));
		                    }
		                }
		    		}
		    	};
    }
    
	/** 
	 * Gets a renderer that draws the proper scale/key/legend for this
	 * HypocentererRenderer based on its current settings.  This uses the Jet
	 * color spectrum in some cases.
	 * @param xStart the x-pixel location for the scale
	 * @param yStart the y-pixel location for the scale
	 * @param kmPerPixel the value for the spatial scale
	 * @return a Renderer that draws the corrent scale/key/legend
	 */
    public Renderer getScaleRenderer(final double xStart, final double yStart, final boolean triple)
    {
        return new Renderer() 
        	{
	            
				/** The render function for the scale renderer.
				 * @param g the graphics object upon which to render
				 */
	            public void render(Graphics2D g)
	            {
	                Object aa = g.getRenderingHint(RenderingHints.KEY_ANTIALIASING);
	                Stroke oldStroke = g.getStroke();
	                g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
	                g.setColor(Color.BLACK);
	                g.setStroke(new BasicStroke(2.0f));
	
	                g.drawString("Magnitude", (float)xStart - 4, (float)yStart - 40);
	                Font origFont = g.getFont();
	                g.setFont(new Font("Arial", Font.PLAIN, 10));
	                for (int i = 0; i < HypocenterRenderer.circles.length; i++)
	                {
	                    //g.translate(xStart + 30 * i, HypocenterRenderer.circles[i].y + yStart);
	                    g.translate(xStart + circleScaleOffset[i], HypocenterRenderer.circles[i].y + yStart);
	                    g.setPaint(Color.WHITE);
	                    g.fill(HypocenterRenderer.circles[i]);
	                    g.setPaint(Color.BLACK);
	                    g.draw(HypocenterRenderer.circles[i]);
	                    //g.translate(-30 * i - xStart, -(HypocenterRenderer.circles[i].y + yStart));
	                    g.translate(-circleScaleOffset[i] - xStart, -(HypocenterRenderer.circles[i].y + yStart));
	                    //g.drawString("" + i, (float)xStart + 30 * i - 3, 15 + (float)yStart);
	                    g.drawString("" + i, (float)xStart + circleScaleOffset[i] - 3, 15 + (float)yStart);
	                }
	                Rectangle2D.Double rect = new Rectangle2D.Double();
	                //double xoff = 270 + xStart;
	                double xoff = 190 + xStart;
	                double yoff = yStart - 65;
	                g.setStroke(new BasicStroke(1.0f));
	                if (colorOpt == ColorOption.DEPTH || axes == Axes.TRIPLE_VIEW)
	                {
	                	float dy = 0;
	                	if (axes == Axes.TRIPLE_VIEW)
	                		dy = -60;
	                    for (int i = 1; i < depths.length - 2; i++)
	                    {
	                        rect.setRect(xoff - 15, yoff + dy, 10, 13);
	                        g.drawString("" + Math.round(-depths[i]), (float)xoff, (float)yoff + 4 + dy);
	                        g.setPaint(colors[i]);
	                        g.fill(rect);
	                        g.setPaint(Color.BLACK);
	                        g.draw(rect);
	                        yoff += 13;
	                    }
	                    g.drawString("" + Math.round(-depths[depths.length - 2]), (float)xoff, (float)yoff + 4 + dy);
	                    //g.drawString("Depth (km)", (float)xStart + 240, (float)yStart + 15);
	                    g.drawString("Depth (km)", (float)xStart + 160, (float)yStart + 15 + dy);
	                    
	                }
	                
	                xoff = 190 + xStart;
	                yoff = yStart - 65;
	                if (colorOpt == ColorOption.TIME || axes == Axes.TRIPLE_VIEW)
	                {
	                	if (axes == Axes.TRIPLE_VIEW)
	                		yoff += 45;
	                	spectrum.renderScale(g, xoff - 15, yoff, 10, 80, true, true);
	                    Object[] t = SmartTick.autoTimeTick(minTime, maxTime, 6);
	                    double[] ticks = (double[])t[0];
	                    String[] labels = (String[])t[1];
	                    double dt = maxTime - minTime;
	                    Line2D.Double line = new Line2D.Double();
	                    for (int i = 0; i < ticks.length; i++)
	                    {
	                    	double yl = ((ticks[i] - minTime) / dt) * 80;
	                        line.setLine(xoff - 5, yoff + yl, xoff - 3, yoff + yl);
	                        g.draw(line);
	                        g.drawString(labels[i], (float)xoff, (float)(yl + yoff + 3));
	                    }
	                }
	                g.setFont(origFont);
	                g.setStroke(new BasicStroke(2.0f));
	                
	                if (axes == Axes.TRIPLE_VIEW)
	                {
		                g.drawString("Top color scale for map view,", (float)xStart, (float)yStart + 35);
		                g.drawString("bottom for depth views.", (float)xStart, (float)yStart + 50);
	                }
	                g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, aa);   
	                g.setStroke(oldStroke);
	            }
        	};
    }
    
	/** 
	 * Renders according to the settings of this HypocenterRenderer.
	 * @param g the graphics object upon which to render
	 */
    public void render(Graphics2D g)
    {
        Color origColor = g.getColor();
        AffineTransform origAT = g.getTransform();
        Stroke origStroke = g.getStroke();
        Font origFont = g.getFont();
        Object aa = g.getRenderingHint(RenderingHints.KEY_ANTIALIASING);
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setStroke(new BasicStroke(2.0f));
        double xt = 0;
        double yt = 0;
        Color color = null;
        double dt = maxTime - minTime;
        List<Hypocenter> hcs = data.getHypocenters();
        for (int i = 0; i < hcs.size(); i++)
        {
        	Hypocenter hc = hcs.get(i);
            if (hc.prefmag != -99.99)
            {
            	switch(axes)
            	{
	            	case MAP_VIEW:
	            	case TRIPLE_VIEW:
	            		xt = transformer.getXPixel(hc.lon);
	                    yt = transformer.getYPixel(hc.lat);
	                    break;
	            	case LON_DEPTH:
	            		xt = transformer.getXPixel(hc.lon);
	                    yt = transformer.getYPixel(hc.depth);
	                    break;
	            	case LAT_DEPTH:
	            		xt = transformer.getXPixel(hc.lat);
	                    yt = transformer.getYPixel(hc.depth);
	            		break;
	            	case DEPTH_TIME:
	            		xt = transformer.getXPixel(hc.j2ksec);
	                    yt = transformer.getYPixel(hc.depth);
	            		break;
            	}
                g.translate(xt, yt);
                
                switch(colorOpt)
                {
	                case DEPTH:
	                	for (int j = 0; j < depths.length - 1; j++)
	                	{
	                        if (hc.depth <= depths[j] && hc.depth > depths[j + 1])
	                        {
	                            color = colors[j];
	                            break;
	                        }
	                	}
	                	break;
	                case TIME:
	                	int ci = (int)(((hc.j2ksec - minTime) / dt) * ((double)spectrum.colors.length - 1));
	                    color = spectrum.colors[ci];
	                	break;
	                case MONOCHROME:
	                	color = Color.BLUE;
	                	break;
                }
                
                g.setColor(color);
				int ci = (int)Math.floor(hc.prefmag);
				if (ci < 0)
					ci = 0;
                g.draw(circles[ci]);
                g.translate(-xt, -yt);
            }
        }
        
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, aa);
        
        g.setColor(origColor);
        g.setTransform(origAT);
        
        if (colorScaleRenderer != null)
        	colorScaleRenderer.render(g);
        if (magnitudeScaleRenderer != null)
        	magnitudeScaleRenderer.render(g);
        
        g.setStroke(origStroke);
        g.setFont(origFont);
    }
    
}
