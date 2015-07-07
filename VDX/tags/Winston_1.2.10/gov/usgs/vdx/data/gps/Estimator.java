package gov.usgs.vdx.data.gps;

import cern.colt.matrix.*;
import cern.colt.matrix.linalg.*;

public class Estimator {
   
    public DoubleMatrix2D WG;
    public DoubleMatrix2D Wd;
    public DoubleMatrix2D m;
    public DoubleMatrix2D dhat;
    public DoubleMatrix2D mcov;
    public DoubleMatrix2D r;
    public double chi2;
	
    public Estimator(DoubleMatrix2D G, DoubleMatrix2D d, DoubleMatrix2D dcov) {		


    	double sxx, syy, szz, sxy, sxz, syz;
    	double w1, w2, w3, w4, w5, w6;
    	double t1, t2;
    	double M;

    	WG = G.copy();
    	Wd = DoubleFactory2D.dense.make(WG.rows(),1);

    	for (int i = 0; i < WG.rows()/3; i++)
    	{
	
			sxx = dcov.getQuick(i, 0);
			syy = dcov.getQuick(i, 1);
			szz = dcov.getQuick(i, 2);
			sxy = dcov.getQuick(i, 3);
			sxz = dcov.getQuick(i, 4);
			syz = dcov.getQuick(i, 5);

			t1 = (sxy*sxy - sxx*syy);
		    t2 = Math.sqrt((sxz*sxz*syy - 2*sxy*sxz*syz + sxx*syz*syz)/(sxy*sxy - sxx*syy) + szz);
		    w1 = 1/Math.sqrt(sxx);
		    w2 = -sxy/sxx/Math.sqrt(syy - sxy*sxy/sxx);
		    w3 = (sxz*syy - sxy*syz)/t1/t2;
		    w4 = 1/Math.sqrt(syy - sxy*sxy/sxx);
		    w5 = (sxx*syz - sxy*sxz)/t1/t2;
		    w6 = 1/t2;

		    Wd.setQuick(i*3, 0, d.getQuick(i, 0)*w1);
		    Wd.setQuick(i*3 + 1, 0, d.getQuick(i, 0)*w2 + d.getQuick(i, 1)*w4);
		    Wd.setQuick(i*3 + 2, 0, d.getQuick(i, 0)*w3 + d.getQuick(i, 1)*w5+ d.getQuick(i, 2)*w6);

		    for (int j = 0; j < WG.columns()/3; j++)
		    {
		    	M = G.getQuick(i*3, j*3);
		    	WG.setQuick(i*3, j*3, M*w1);
		    	WG.setQuick(i*3 + 1, j*3, M*w2);
		    	WG.setQuick(i*3 + 2, j*3, M*w3);
		    	WG.setQuick(i*3 + 1, j*3 + 1, M*w4);
		    	WG.setQuick(i*3 + 2, j*3 + 1, M*w5);
		    	WG.setQuick(i*3 + 2, j*3 + 2, M*w6);
		    }
    	}
    }
    
    public void solve()
    {
    	mcov = Algebra.DEFAULT.inverse(Algebra.DEFAULT.mult(Algebra.DEFAULT.transpose(WG),WG));
    	m = Algebra.DEFAULT.mult(Algebra.DEFAULT.mult(mcov,Algebra.DEFAULT.transpose(WG)),Wd);
    	dhat = Algebra.DEFAULT.mult(WG, m);
    	r = dhat.copy();
    	for (int i = 0; i < Wd.rows(); i++)
    		r.setQuick(i,0,Wd.getQuick(i,0) - dhat.getQuick(i,0));
    	chi2 = Algebra.DEFAULT.mult(r.viewDice(),r).getQuick(0,0) /(Wd.rows() - 6);
    }
    	
    public DoubleMatrix2D getModel()
    {
    	return m;
    }
    
    public DoubleMatrix2D getModelCovariance()
    {
    	return mcov;
    }
    
    public DoubleMatrix2D getPrediction()
    {
    	return dhat;
    }
    
    public DoubleMatrix2D getResidual()
    {
    	return r;
    }
    
    public double getChi2()
    {
    	return chi2;
    }
    
}
