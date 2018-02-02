package gov.usgs.volcanoes.vdx.data.gps;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.linalg.Algebra;

public class Estimator {

  public DoubleMatrix2D wg;
  public DoubleMatrix2D wd;
  public DoubleMatrix2D model;
  public DoubleMatrix2D dhat;
  public DoubleMatrix2D mcov;
  public DoubleMatrix2D residual;
  public double chi2;

  /**
   * Constructor.
   */
  public Estimator(DoubleMatrix2D dkernel, DoubleMatrix2D dpos, DoubleMatrix2D dcov) {

    double sxx;
    double syy;
    double szz;
    double sxy;
    double sxz;
    double syz;
    double w1;
    double w2;
    double w3;
    double w4;
    double w5;
    double w6;
    double t1;
    double t2;
    double m;

    wg = dkernel.copy();
    wd = DoubleFactory2D.dense.make(wg.rows(), 1);

    for (int i = 0; i < wg.rows() / 3; i++) {

      sxx = dcov.getQuick(i, 0);
      syy = dcov.getQuick(i, 1);
      szz = dcov.getQuick(i, 2);
      sxy = dcov.getQuick(i, 3);
      sxz = dcov.getQuick(i, 4);
      syz = dcov.getQuick(i, 5);

      t1 = (sxy * sxy - sxx * syy);
      t2 = Math.sqrt(
          (sxz * sxz * syy - 2 * sxy * sxz * syz + sxx * syz * syz) / (sxy * sxy - sxx * syy)
              + szz);
      w1 = 1 / Math.sqrt(sxx);
      w2 = -sxy / sxx / Math.sqrt(syy - sxy * sxy / sxx);
      w3 = (sxz * syy - sxy * syz) / t1 / t2;
      w4 = 1 / Math.sqrt(syy - sxy * sxy / sxx);
      w5 = (sxx * syz - sxy * sxz) / t1 / t2;
      w6 = 1 / t2;

      wd.setQuick(i * 3, 0, dpos.getQuick(i, 0) * w1);
      wd.setQuick(i * 3 + 1, 0, dpos.getQuick(i, 0) * w2 + dpos.getQuick(i, 1) * w4);
      wd.setQuick(i * 3 + 2, 0,
          dpos.getQuick(i, 0) * w3 + dpos.getQuick(i, 1) * w5 + dpos.getQuick(i, 2) * w6);

      for (int j = 0; j < wg.columns() / 3; j++) {
        m = dkernel.getQuick(i * 3, j * 3);
        wg.setQuick(i * 3, j * 3, m * w1);
        wg.setQuick(i * 3 + 1, j * 3, m * w2);
        wg.setQuick(i * 3 + 2, j * 3, m * w3);
        wg.setQuick(i * 3 + 1, j * 3 + 1, m * w4);
        wg.setQuick(i * 3 + 2, j * 3 + 1, m * w5);
        wg.setQuick(i * 3 + 2, j * 3 + 2, m * w6);
      }
    }
  }

  /**
   * Solve.
   */
  public void solve() {
    mcov = Algebra.DEFAULT.inverse(Algebra.DEFAULT.mult(Algebra.DEFAULT.transpose(wg), wg));
    model = Algebra.DEFAULT.mult(Algebra.DEFAULT.mult(mcov, Algebra.DEFAULT.transpose(wg)), wd);
    dhat = Algebra.DEFAULT.mult(wg, model);
    residual = dhat.copy();
    for (int i = 0; i < wd.rows(); i++) {
      residual.setQuick(i, 0, wd.getQuick(i, 0) - dhat.getQuick(i, 0));
    }
    chi2 = Algebra.DEFAULT.mult(residual.viewDice(), residual).getQuick(0, 0) / (wd.rows() - 6);
  }

  public DoubleMatrix2D getModel() {
    return model;
  }

  public DoubleMatrix2D getModelCovariance() {
    return mcov;
  }

  public DoubleMatrix2D getPrediction() {
    return dhat;
  }

  public DoubleMatrix2D getResidual() {
    return residual;
  }

  public double getChi2() {
    return chi2;
  }

}
