package gov.usgs.volcanoes.vdx.data.hypo.plot;

import gov.usgs.volcanoes.core.legacy.plot.color.Jet;
import gov.usgs.volcanoes.core.legacy.plot.color.Spectrum;
import gov.usgs.volcanoes.core.legacy.plot.decorate.SmartTick;
import gov.usgs.volcanoes.core.legacy.plot.render.ArbDepthFrameRenderer;
import gov.usgs.volcanoes.core.legacy.plot.render.Renderer;
import gov.usgs.volcanoes.core.legacy.plot.transform.ArbDepthCalculator;
import gov.usgs.volcanoes.core.legacy.plot.transform.Transformer;
import gov.usgs.volcanoes.vdx.data.hypo.Hypocenter;
import gov.usgs.volcanoes.vdx.data.hypo.HypocenterList;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
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
 * HypocenterRenderer is a class that render a list of hypocenters in a variety of different ways.
 *
 * @author Dan Cervelli
 */
public class HypocenterRenderer implements Renderer {

  /**
   * Enumeration to define scale type.
   */
  public enum AxesOption {
    MAP_VIEW, LON_DEPTH, LAT_DEPTH, TIME_DEPTH, TRIPLE_VIEW, ARB_DEPTH, ARB_TIME;

    /**
     * Create AxesOption from string.
     */
    public static AxesOption fromString(String c) {
      if (c == null) {
        return null;
      }
      switch (c.charAt(0)) {
        case 'M':
          return MAP_VIEW;
        case 'E':
          return LON_DEPTH;
        case 'N':
          return LAT_DEPTH;
        case 'D':
          return TIME_DEPTH;
        case 'A':
          return ARB_DEPTH;
        case 'T':
          return ARB_TIME;
        case '3':
          return TRIPLE_VIEW;

        default:
          return null;
      }
    }
  }

  /**
   * Enumeration to define coloring mode.
   */
  public enum ColorOption {
    DEPTH, TIME, MONOCHROME, OLD;

    /**
     * Create ColorOption from string.
     */
    public static ColorOption fromString(String c) {
      if (c == null) {
        return null;
      }
      switch (c.charAt(0)) {
        case 'D':
          return DEPTH; //Coloring by depth
        case 'T':
          return TIME;  //Coloring by time
        case 'M':
          return MONOCHROME;
        case 'O':
          return OLD;
        default:
          return null;
      }
    }

    /**
     * Auto-choose color option based on view.
     */
    public static ColorOption chooseAuto(AxesOption ax) {
      if (ax == null) {
        return null;
      }

      switch (ax) {
        case MAP_VIEW:
        case TIME_DEPTH:
        case ARB_TIME:
          return DEPTH;

        case LON_DEPTH:
        case LAT_DEPTH:
        case ARB_DEPTH:
          return TIME;

        default:
          return MONOCHROME;
      }
    }
  }

  private HypocenterList data;
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
   * The shapes that used to render different magnitude earthquakes.
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

  public static final int[] circleScaleOffset = new int[]{0, 30, 60, 90, 120, 150, 180, 210};

  /**
   * The colors for different depths.
   */
  public static Color[] colors = new Color[] {Color.BLACK, Color.RED, new Color(1.0f, 0.5f, 0.1f),
      new Color(1.0f, 0.91f, 0.0f), new Color(0.0f, 0.6f, 0.0f), Color.BLUE};
  public static Color[] origColors = new Color[] {Color.BLACK, Color.GREEN, Color.RED,
      new Color(1.0f, 0.91f, 0.0f), Color.BLUE, new Color(0.8f, 0.0f, 1.0f)};

  /**
   * The depth intervals.
   */
  public static double[] depths = new double[]{-10000, 0, 5, 13, 20, 40, 10000};
  public static String[] depthStrings = new String[]{"<0", "0", "5", "13", "20", "40", "70+"};

  static {
    numberFormat = NumberFormat.getInstance();
    numberFormat.setMaximumFractionDigits(0);
  }

  /**
   * Constructor for HypocenterRenderer that gets the data, the coordinate transformer and the view
   * type.  The different views are:<br> M: map view (axes=lon/lat)<br> E or A: east view
   * (axes=lon/depth)<br> N: north view (axes=lat/depth)<br> T: time view (axes=time/lon)<br>
   *
   * @param d the earthquake data
   * @param fr the coordinate transformer (usually a FrameRenderer)
   * @param v the view type
   */
  public HypocenterRenderer(HypocenterList d, Transformer fr, AxesOption v) {
    data = d;
    transformer = fr;
    axesOption = v;
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
    //if (axes == Axes.TRIPLE_VIEW)
    //  colorOption = "T";
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
   * Sets depth color mode.
   */
  public void setColorDepth() {
    colorOption = ColorOption.DEPTH;
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
  public void createMagnitudeScaleRenderer(final double startX, final double startY) {
    magnitudeScaleRenderer = new Renderer() {
      public void render(Graphics2D g) {
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setColor(Color.BLACK);
        g.setStroke(new BasicStroke(2.0f));
        g.setFont(new Font("Arial", Font.PLAIN, 10));
        FontMetrics metrics = g.getFontMetrics(g.getFont());

        g.drawString("Magnitude", (float) startX - 80, (float) startY);

        for (int i = 0; i < HypocenterRenderer.circles.length; i++) {
          g.translate(startX - HypocenterRenderer.circles[i].x + circleScaleOffset[i],
              startY + HypocenterRenderer.circles[i].y - 15);
          g.setPaint(Color.WHITE);
          g.fill(HypocenterRenderer.circles[i]);
          g.setPaint(Color.BLACK);
          g.draw(HypocenterRenderer.circles[i]);
          g.drawString("" + i, -(metrics.stringWidth("" + i) / 2),
              -HypocenterRenderer.circles[i].y + 15);
          g.translate(-(startX - HypocenterRenderer.circles[i].x + circleScaleOffset[i]),
              -(startY + HypocenterRenderer.circles[i].y - 15));
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
        if (colorOption == ColorOption.DEPTH || colorOption == ColorOption.OLD) {
          Rectangle2D.Double rect = new Rectangle2D.Double();
          double yoff = 0;
          double ty = (depths.length - 1) * 18;

          g.drawString("Depth (km)", (float) startX + 60, (float) startY);

          for (int i = 0; i < depths.length - 1; i++) {
            rect.setRect(startX, startY - ty + yoff, 10, 18);
            g.drawString(depthStrings[i], (float) startX + 18, (float) (startY - ty + 4 + yoff));
            if (colorOption == ColorOption.OLD) {
              g.setPaint(origColors[i]);
            } else {
              g.setPaint(colors[i]);
            }
            g.fill(rect);
            g.setPaint(Color.BLACK);
            g.draw(rect);
            yoff += 18;
          }
          g.drawString(depthStrings[depths.length - 1], (float) startX + 18, (float) startY);

        }

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
    final Stroke origStroke = g.getStroke();
    final Font origFont = g.getFont();
    final Object aa = g.getRenderingHint(RenderingHints.KEY_ANTIALIASING);
    g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
    g.setStroke(new BasicStroke(2.0f));
    double xt = 0;
    double yt = 0;
    Color color = null;
    double dt = maxTime - minTime;
    List<Hypocenter> hcs = data.getHypocenters();
    ArbDepthFrameRenderer adfr = null;

    int skippedCount = 0;

    for (int i = 0; i < hcs.size(); i++) {
      Hypocenter hc = hcs.get(i);
      if (hc.prefmag != -99.99) {
        switch (axesOption) {
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
          case ARB_DEPTH:
            // this is where we must convert from lat/lon to distance along a line.
            // for the xt point.

            // if my transformer is really of type ArbDepthFrameRenderer,
            // then use the embedded ArbDepthCalculator
            adfr = null;

            if (transformer instanceof ArbDepthFrameRenderer) {
              adfr = (ArbDepthFrameRenderer) transformer;
              ArbDepthCalculator adc = adfr.getArbDepthCalc();

              if (adc != null) {

                double projectedDist = adc.getScaledProjectedDistance(hc.lat, hc.lon);

                if (projectedDist < 0) {
                  skippedCount++;
                  continue;
                }

                if (projectedDist > adc.getMaxDist()) {
                  skippedCount++;
                  continue;
                }

                double projectedWidth = Math.abs(adc.getScalePojectedWidth(hc.lat, hc.lon));
                double tmpWidth = adc.getWidth();
                if (tmpWidth > 0.0) {
                  if (projectedWidth > tmpWidth) {
                    skippedCount++;
                    continue;
                  }
                }

                xt = transformer.getXPixel(projectedDist);
              } else {
                // we don't have the arbitrary depth calculator, just plot the point at zero.
                xt = 0.0;
              }


            } else {
              xt = transformer.getXPixel(hc.lat);
            }
            yt = transformer.getYPixel(hc.depth);
            break;
          case TIME_DEPTH:
            xt = transformer.getXPixel(hc.j2ksec);
            yt = transformer.getYPixel(hc.depth);
            break;

          case ARB_TIME:
            // this is where we must convert from lat/lon to distance along a line.
            // for the xt point.

            // if my transformer is really of type ArbDepthFrameRenderer,
            // then use the embedded ArbDepthCalculator
            adfr = null;

            if (transformer instanceof ArbDepthFrameRenderer) {
              adfr = (ArbDepthFrameRenderer) transformer;
              ArbDepthCalculator adc = adfr.getArbDepthCalc();

              if (adc != null) {

                double projectedDist = adc.getScaledProjectedDistance(hc.lat, hc.lon);

                if (projectedDist < 0) {
                  skippedCount++;
                  continue;
                }

                if (projectedDist > adc.getMaxDist()) {
                  skippedCount++;
                  continue;
                }

                double projectedWidth = Math.abs(adc.getScalePojectedWidth(hc.lat, hc.lon));
                double tmpWidth = adc.getWidth();
                if (tmpWidth > 0.0) {
                  if (projectedWidth > tmpWidth) {
                    skippedCount++;
                    continue;
                  }
                }

                yt = transformer.getYPixel(projectedDist);
              } else {
                // we don't have the arbitrary depth calculator, just plot the point at zero.
                yt = 0.0;
              }


            } else {
              yt = transformer.getXPixel(hc.lat);
            }
            xt = transformer.getXPixel(hc.j2ksec);
            break;
          default:
            break;
        }
        g.translate(xt, yt);

        switch (colorOption) {
          case OLD:
          case DEPTH:
            for (int j = 0; j < depths.length - 1; j++) {
              if (hc.depth >= depths[j] && hc.depth < depths[j + 1]) {
                if (colorOption == ColorOption.OLD) {
                  color = origColors[j];
                } else {
                  color = colors[j];
                }
                break;
              }
            }
            break;
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
        int ci = (int) Math.floor(hc.prefmag);
        if (ci < 0) {
          ci = 0;
        }
        g.draw(circles[ci]);
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
