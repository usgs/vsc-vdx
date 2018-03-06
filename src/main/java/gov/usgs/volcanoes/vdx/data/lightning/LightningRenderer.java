package gov.usgs.volcanoes.vdx.data.lightning;

import gov.usgs.volcanoes.core.legacy.plot.color.Jet;
import gov.usgs.volcanoes.core.legacy.plot.color.Spectrum;
import gov.usgs.volcanoes.core.legacy.plot.decorate.SmartTick;
import gov.usgs.volcanoes.core.legacy.plot.render.ArbDepthFrameRenderer;
import gov.usgs.volcanoes.core.legacy.plot.render.Renderer;
import gov.usgs.volcanoes.core.legacy.plot.transform.Transformer;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.AffineTransform;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Line2D;
import java.text.NumberFormat;
import java.util.List;

/**
 * LightningRenderer is a class that render a list of lightning strikes in a variety of different
 * ways. Modeled after HypocenterPlotter.
 *
 * @author Tom Parker
 */
public class LightningRenderer implements Renderer {

  /**
   * Enumeration to define scale type.
   */
  public enum AxesOption {
    MAP_VIEW;

    /**
     * Construct AxesOption from string.
     */
    public static AxesOption fromString(String c) {
      if (c == null) {
        return null;
      }
      switch (c.charAt(0)) {
        case 'M':
          return MAP_VIEW;

        default:
          return null;
      }
    }
  }

  /**
   * Enumeration to define coloring mode.
   */
  public enum ColorOption {
    TIME, MONOCHROME;

    /**
     * Construct ColorOption from string.
     */
    public static ColorOption fromString(String c) {
      if (c == null) {
        return null;
      }
      switch (c.charAt(0)) {
        case 'T':
          return TIME;  //Coloring by time
        case 'M':
          return MONOCHROME;
        default:
          return null;
      }
    }

    /**
     * Choose color option based on view.
     */
    public static ColorOption chooseAuto(AxesOption ax) {
      if (ax == null) {
        return null;
      }

      switch (ax) {
        case MAP_VIEW:
          return TIME;

        default:
          return MONOCHROME;
      }
    }
  }

  public static enum StrokeCircle {
    ONE("1", 30, new Ellipse2D.Float(-1.5f, -1.5f, 3, 3)),
    LESS5("<5", 60, new Ellipse2D.Float(-3, -3, 6, 6)),
    LESS10("<10", 90, new Ellipse2D.Float(-5.5f, -5.5f, 11, 11)),
    TEN_PLUS("10+", 120, new Ellipse2D.Float(-9, -9, 18, 18));

    public String label;
    public Ellipse2D.Float circle;
    public int offset;

    private StrokeCircle(String label, int offset, Ellipse2D.Float circle) {
      this.label = label;
      this.offset = offset;
      this.circle = circle;
    }

    /**
     * Create StrokeCircle based on int.
     */
    public static StrokeCircle fromInt(int numStations) {
      if (numStations == 1) {
        return ONE;
      } else if (numStations < 5) {
        return LESS5;
      } else if (numStations < 10) {
        return LESS10;
      } else {
        return TEN_PLUS;
      }
    }
  }

  private StrokeList data;
  private Transformer transformer;
  private AxesOption axesOption;
  private static NumberFormat numberFormat;
  private ColorOption colorOption;
  private double minTime;
  private double maxTime;
  private static final Spectrum spectrum = Jet.getInstance();

  private Renderer colorScaleRenderer;
  private Renderer magnitudeScaleRenderer;

  /**
   * The colors for different depths.
   */
  public static Color[] colors = new Color[]{Color.BLACK, Color.GREEN, Color.RED,
      new Color(1.0f, 0.91f, 0.0f), Color.BLUE, new Color(0.8f, 0f, 1f)};

  /**
   * The depth intervals.
   */
  public static double[] depths = new double[]{-10000, 0, 5, 13, 20, 40, 10000};
  public static String[] depthStrings = new String[]{"", "0", "5", "13", "20", "40", "70+"};

  /**
   * Constructor for HypocenterRenderer that gets the data, the coordinate transformer and the view
   * type.  The different views are:<br> M: map view (axes=lon/lat)<br> E or A: east view
   * (axes=lon/depth)<br> N: north view (axes=lat/depth)<br> T: time view (axes=time/lon)<br>
   *
   * @param d the earthquake data
   * @param fr the coordinate transformer (usually a FrameRenderer)
   * @param v the view type
   */
  public LightningRenderer(StrokeList d, Transformer fr, AxesOption v) {
    data = d;
    transformer = fr;
    axesOption = v;
    if (numberFormat == null) {
      numberFormat = NumberFormat.getInstance();
      numberFormat.setMaximumFractionDigits(0);
    }
    colorOption = ColorOption.chooseAuto(v);
  }

  /**
   * Sets the renderer to operate in color-time mode where time is represented in color instead of
   * depth.
   *
   * @param min the minimum time (j2ksec) for the coolest color
   * @param max the maximum time (j2ksec) for the warmest color
   */
  public void setColorTime(double min, double max) {
    // if (axes == Axes.TRIPLE_VIEW)
    //   colorOption = "T";
    colorOption = ColorOption.TIME;
    minTime = min;
    maxTime = max;
  }

  /**
   * Sets monochrome color mode.
   */
  public void setMonochrome() {
    colorOption = ColorOption.MONOCHROME;
  }

  /**
   * Sets color mode.
   *
   * @param c Color option to set
   */
  public void setColorOption(ColorOption c) {
    colorOption = c;
  }

  /**
   * Create renderer to draw hypocenter's magnitude.
   *
   * @param startX hypocenter's X pixel coordinate
   * @param startY hypocenter's Y pixel coordinate
   */
  public void createStrokeScaleRenderer(final double startX, final double startY) {
    magnitudeScaleRenderer = new Renderer() {
      public void render(Graphics2D g) {
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setColor(Color.BLACK);
        g.setStroke(new BasicStroke(2.0f));
        g.setFont(new Font("Arial", Font.PLAIN, 10));
        FontMetrics metrics = g.getFontMetrics(g.getFont());

        g.drawString("# Stations Detected", (float) startX - 80, (float) startY);

        int count = 0;
        for (StrokeCircle strokeCircle : StrokeCircle.values()) {
          Ellipse2D.Float circle = strokeCircle.circle;
          String label = strokeCircle.label;
          int offset = strokeCircle.offset;

          g.translate(startX - circle.x + offset, startY + circle.y - 15);
          g.setPaint(Color.WHITE);
          g.fill(circle);
          g.setPaint(Color.BLACK);
          g.draw(circle);
          g.drawString(label, -(metrics.stringWidth(label) / 2), -circle.y + 15);
          g.translate(-(startX - circle.x + offset), -(startY + circle.y - 15));
        }
      }
    };
  }

  /**
   * Create renderer to draw hypocenter in current color mode and scale type.
   *
   * @param startX hypocenter's X pixel coordinate
   * @param startY hypocenter's Y pixel coordinate
   */
  public void createColorScaleRenderer(final double startX, final double startY) {
    colorScaleRenderer = new Renderer() {
      public void render(Graphics2D g) {
        g.setStroke(new BasicStroke(1.0f));
        g.setFont(new Font("Arial", Font.PLAIN, 10));

        if (colorOption == ColorOption.TIME) {
          spectrum.renderScale(g, startX, startY - 90, 10, 90, true, true);
          Object[] t = SmartTick.autoTimeTick(minTime, maxTime, 5);
          double[] ticks = (double[]) t[0];
          String[] labels = (String[]) t[1];
          double dt = maxTime - minTime;
          Line2D.Double line = new Line2D.Double();

          g.drawString("Time", (float) startX + 60, (float) startY);

          for (int i = 0; i < ticks.length; i++) {
            double yl = ((ticks[i] - minTime) / dt) * 90;
            line.setLine(startX + 10, startY - 90 + yl, startX + 13, startY - 90 + yl);
            g.draw(line);
            g.drawString(labels[i], (float) startX + 16, (float) (yl + startY - 90 + 3));
          }
        }
      }
    };
  }

  /**
   * Renders according to the settings of this HypocenterRenderer.
   *
   * @param g the graphics object upon which to render
   */
  public void render(Graphics2D g) {
    final Color origColor = g.getColor();
    final AffineTransform origAT = g.getTransform();
    final java.awt.Stroke origStroke = g.getStroke();
    final Font origFont = g.getFont();
    final Object aa = g.getRenderingHint(RenderingHints.KEY_ANTIALIASING);
    g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
    g.setStroke(new BasicStroke(2.0f));
    double xt = 0;
    double yt = 0;
    Color color = null;
    double dt = maxTime - minTime;
    List<Stroke> hcs = data.getStrokes();
    ArbDepthFrameRenderer adfr = null;

    int skippedCount = 0;

    for (int i = 0; i < hcs.size(); i++) {
      Stroke hc = hcs.get(i);
      {
        switch (axesOption) {
          case MAP_VIEW:
            xt = transformer.getXPixel(hc.lon);
            yt = transformer.getYPixel(hc.lat);
            break;
          default:
            break;
        }
        g.translate(xt, yt);

        switch (colorOption) {
          case TIME:
            int ci = (int) (((hc.j2ksec - minTime) / dt) * ((double) spectrum.colors.length - 1));
            color = spectrum.colors[ci];
            break;
          case MONOCHROME:
            color = Color.BLUE;
            break;
          default:
            break;
        }

        g.setColor(color);
        g.draw(StrokeCircle.fromInt(hc.stationsDetected).circle);
        g.translate(-xt, -yt);
      }
    }

    g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, aa);

    g.setColor(origColor);
    g.setTransform(origAT);

    if (colorScaleRenderer != null) {
      colorScaleRenderer.render(g);
    }
    if (magnitudeScaleRenderer != null) {
      magnitudeScaleRenderer.render(g);
    }

    g.setStroke(origStroke);
    g.setFont(origFont);
  }

}
