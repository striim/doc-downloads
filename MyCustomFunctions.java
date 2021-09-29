package com.webaction.custom.mycfs;

import com.webaction.runtime.compiler.custom.AggHandlerDesc;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.lang.Math;
import java.util.LinkedList;
import org.joda.time.DateTime;

public abstract class MyCustomFunctions {

    private static final Logger logger = LogManager.getLogger( MyCustomFunctions.class );

    public static double maxSingleRow(double... n) {
        int i = 0;
        double runningmax = n[i];

        while (++i < n.length)
            if (n[i] > runningmax)
                runningmax = n[i];

        return runningmax;
    }   
    
    public static double minSingleRow(double... n) {
        int i = 0;
        double runningmin = n[i];

        while (++i < n.length)
            if (n[i] < runningmin)
                runningmin = n[i];

        return runningmin;
    }
    
    
    public static double rangeSingleRow(double... n) {
        double dRange = maxSingleRow( n ) - minSingleRow( n );

        return dRange;
    }   

    public static double avgSingleRow(double... n) {
        int i = 0;
        double runningsum = 0;

        while (i < n.length) {
            runningsum += n[i];
            i++;
        }

        return ( runningsum / i );
    }   


    
    @AggHandlerDesc(handler=RChart_Double.class)
    public abstract java.lang.Double[] RChart( double... n );
    public static class RChart_Double {
        // inc/decAggValue calculated variables (calculated row-by-row)
        double  dRowMean = 0;           // the average of the observations across each row. Also known as "Xbar"
        double  dRowRange = 0;          // the difference between the Min and Max observation across each row. Also known as "R"
        double  dRunningSumMean = 0;    // (intermediary calculation)
        double  dRunningSumRange = 0;   // (intermediary calculation)
        double  dRunningMeanMean = 0;   // also known as "Xdoublebar"
        double  dRunningMeanRange = 0;  // also known as "Rbar"
        int     iRunningCount = 0;
        int     iNumObs = 0;            // also known as the "subgroup size"
        
        
        final static double[] A2 = { Double.NaN, Double.NaN, 1.880,  1.023, 0.729, 0.577, 0.483, 0.419, 0.373, 0.337, 0.308, 0.285, 0.266, 0.249, 0.235, 0.223, 0.212, 0.203, 0.194, 0.187, 0.180, 0.173, 0.167, 0.162, 0.157, 0.153 };
        final static double[] D3 = { Double.NaN, Double.NaN, 0, 0, 0, 0, 0, 0.076, 0.136, 0.184, 0.223, 0.256, 0.284, 0.308, 0.329, 0.348, 0.364, 0.379, 0.392, 0.404, 0.414, 0.425, 0.434, 0.443, 0.452, 0.459 };
        final static double[] D4 = { Double.NaN, Double.NaN, 3.267, 2.574, 2.282, 2.114, 2.004, 1.924, 1.864, 1.816, 1.777, 1.774, 1.716, 1.692, 1.671, 1.652, 1.636, 1.621, 1.608, 1.596, 1.586, 1.575, 1.566, 1.557, 1.548, 1.541 }; 
        
        
        public java.lang.Double[] getAggValue() { 
            /*********************************************************************************************************************
             *  Note that (as with all aggregate functions) this method will actually get called *twice* as a window overflows
             *  Be prepared for this while you're debugging!
             *  - The first call (after the incAggValue() method will temporarily result in incorrect figures
             *  - The second call (after the decAggValue() method will result in the correct, final figures 
             */ 

            // Now is the correct time to calculate the averages
            // First, "Xdoublebar", the average of the individual averages - across the window
            dRunningMeanMean  = (iRunningCount == 0) ? 0 : (dRunningSumMean / iRunningCount);
            // Next, "Rbar', the average of the ranges - across the window
            dRunningMeanRange = (iRunningCount == 0) ? 0 : (dRunningSumRange / iRunningCount);
            logger.debug( "dRunningMeanMean: " + dRunningMeanMean + ", dRunningMeanRange: " + dRunningMeanRange );

           
            // Calculate Control Limits, X-bar Chart
            double  dCLx = 0;
            double  dUCLx = 0;
            double  dLCLx = 0;
            double  dPlotx = 0;
            
            dPlotx = dRowMean;  // Xbar
            dCLx = dRunningMeanMean;
            if ( iNumObs > 1 && iNumObs < 26 ) {
                dUCLx = dRunningMeanMean + A2[ iNumObs ] * dRunningMeanRange;   // Xdoublebar + A2 * Rbar
                dLCLx = dRunningMeanMean - A2[ iNumObs ] * dRunningMeanRange;   // Xdoublebar - A2 * Rbar
            } else {
                dUCLx = Double.NaN;
                dLCLx = Double.NaN;            
            }
            logger.debug( "dPlotx: " + dPlotx + ", dCLx: " + dCLx + ", dUCLx: " + dUCLx + ", dLCLx: " + dLCLx );

            //Calculate Control Limits, R Chart (Range Chart)
            double  dCLr = 0;
            double  dUCLr = 0;
            double  dLCLr = 0;
            double  dPlotr = 0;
 
            dPlotr  = dRowRange;
            dCLr    = dRunningMeanRange;  //Rbar
            dUCLr   = D4[ iNumObs ] * dRunningMeanRange;    // D4 * Rbar
            dLCLr   = D3[ iNumObs ] * dRunningMeanRange;    // D3 * Rbar
           
            Double[] r = { dPlotx, dCLx, dUCLx, dLCLx, dPlotr, dCLr, dUCLr, dLCLr };

            return r; 
        }
        public void incAggValue( double... arg ) 
        { 
            if( arg != null ) {
                //Derive the number of observations that make up the sample size (will need this for "A2" later)
                iNumObs = arg.length;

                logInputFigures( iNumObs, arg );

                //Calculate the Averages and Ranges across the row (but not across the multiple rows in the window!)
                dRowMean  = avgSingleRow( arg );    
                dRowRange = rangeSingleRow( arg );
                logger.debug( "dRowMean: " + dRowMean + ", dRowRange: " + dRowRange );
                
                /*********************************************************************************************************************
                 *  Maintaining a Running Sum of the Averages and Ranges, plus a running count - in order to calculate
                 *  the averages across the window - but at a suitable time!
                 *  
                 *  Note:
                 *  As with all aggregate/window functions, the following three figures will temporary be inaccurate as the window overflows.
                 *  But the situation will resolve itself as soon as the 'decAggValue' method gets called on the out-going record
                 */
                iRunningCount++;
                dRunningSumMean += dRowMean;
                dRunningSumRange += dRowRange;
                logger.debug( "iRunningCount: " + iRunningCount + ", dRunningSumMean: " + dRunningSumMean + ", dRunningSumRange: " + dRunningSumRange );
            }
       }
        public void decAggValue( double... arg ) 
        { 
            if( arg != null ) {
                iNumObs = arg.length;
                logInputFigures( iNumObs, arg );

                //Note: Do NOT set the row mean/row range using the "out-going" record.
                // I need the row mean/row range to remain at the "incoming" record's values
                //dRowMean  =   
                //dRowRange = 

                iRunningCount--;    //(effectively this will repeatedly reset itself to the window size)
                dRunningSumMean -= avgSingleRow( arg );     //decrement the running total - without storing the row average
                dRunningSumRange -= rangeSingleRow( arg );  //decrement the running total - without storing the row range
                logger.debug( "iRunningCount: " + iRunningCount + ", dRunningSumMean: " + dRunningSumMean + ", dRunningSumRange: " + dRunningSumRange );
 
            }
        }

        private void logInputFigures( int iNumObs, double... arg ) {
            Level currentLevel = logger.getLevel();
            if ( currentLevel != null && ( currentLevel == Level.DEBUG) ) {
                String s = "";
                for(int i = 0; i < arg.length; i++){
                     s += arg[ i ] + " ";
                 }
                 logger.debug( "Input values (" + iNumObs + "): " + s);
            }           
        }
    }

    
    
    
    
    @AggHandlerDesc(handler=LastBut_String.class)
    public abstract String LastBut(String arg, int idx);
    public static class LastBut_String
    {
        LinkedList<String> list = new LinkedList<String>();
        int i;
        
        public String getAggValue() 
        { 
            if( list.isEmpty()  || (i < 0) || (i > (list.size()-1)) ) { 
                return null;
            } else {
                return list.get( ( list.size()-i-1) );
            }
        }
        public void incAggValue(String arg, int idx) 
        { 
            list.addLast(arg);
            i = idx;  //no safety logic required here - all handled by the getAggValue() method
        }
        public void decAggValue(String arg, int idx) 
        { 
            if(!list.isEmpty()) list.removeFirst();
        }
    }

    @AggHandlerDesc(handler=LastBut_Byte.class)
    public abstract Byte LastBut(Byte arg, int idx);
    public static class LastBut_Byte
    {
        LinkedList<Byte> list = new LinkedList<Byte>();
        int i;
        
        public Byte getAggValue() 
        { 
            if( list.isEmpty()  || (i < 0) || (i > (list.size()-1)) ) { 
                return null;
            } else {
                return list.get( ( list.size()-i-1) );
            }
        }
        public void incAggValue(Byte arg, int idx) 
        { 
            list.addLast(arg);
            i = idx;  //no safety logic required here - all handled by the getAggValue() method
        }
        public void decAggValue(Byte arg, int idx) 
        { 
            if(!list.isEmpty()) list.removeFirst();
        }
    }

    @AggHandlerDesc(handler=LastBut_Short.class)
    public abstract Short LastBut(Short arg, int idx);
    public static class LastBut_Short
    {
        LinkedList<Short> list = new LinkedList<Short>();
        int i;
        
        public Short getAggValue() 
        { 
            if( list.isEmpty()  || (i < 0) || (i > (list.size()-1)) ) { 
                return null;
            } else {
                return list.get( ( list.size()-i-1) );
            }
        }
        public void incAggValue(Short arg, int idx) 
        { 
            list.addLast(arg);
            i = idx;  //no safety logic required here - all handled by the getAggValue() method
        }
        public void decAggValue(Short arg, int idx) 
        { 
            if(!list.isEmpty()) list.removeFirst();
        }
    }


    @AggHandlerDesc(handler=LastBut_Integer.class)
    public abstract Integer LastBut(Integer arg, int idx);
    public static class LastBut_Integer
    {
        LinkedList<Integer> list = new LinkedList<Integer>();
        int i;
        
        public Integer getAggValue() 
        { 
            if( list.isEmpty()  || (i < 0) || (i > (list.size()-1)) ) { 
                return null;
            } else {
                return list.get( ( list.size()-i-1) );
            }
        }
        public void incAggValue(Integer arg, int idx) 
        { 
            list.addLast(arg);
            i = idx;  //no safety logic required here - all handled by the getAggValue() method
        }
        public void decAggValue(Integer arg, int idx) 
        { 
            if(!list.isEmpty()) list.removeFirst();
        }
    }


    @AggHandlerDesc(handler=LastBut_Long.class)
    public abstract Long LastBut(Long arg, int idx);
    public static class LastBut_Long
    {
        LinkedList<Long> list = new LinkedList<Long>();
        int i;
        
        public Long getAggValue() 
        { 
            if( list.isEmpty()  || (i < 0) || (i > (list.size()-1)) ) { 
                return null;
            } else {
                return list.get( ( list.size()-i-1) );
            }
        }
        public void incAggValue(Long arg, int idx) 
        { 
            list.addLast(arg);
            i = idx;  //no safety logic required here - all handled by the getAggValue() method
        }
        public void decAggValue(Long arg, int idx) 
        { 
            if(!list.isEmpty()) list.removeFirst();
        }
    }

    @AggHandlerDesc(handler=LastBut_Float.class)
    public abstract Float LastBut(Float arg, int idx);
    public static class LastBut_Float
    {
        LinkedList<Float> list = new LinkedList<Float>();
        int i;
        
        public Float getAggValue() 
        { 
            if( list.isEmpty()  || (i < 0) || (i > (list.size()-1)) ) { 
                return null;
            } else {
                return list.get( ( list.size()-i-1) );
            }
        }
        public void incAggValue(Float arg, int idx) 
        { 
            list.addLast(arg);
            i = idx;  //no safety logic required here - all handled by the getAggValue() method
        }
        public void decAggValue(Float arg, int idx) 
        { 
            if(!list.isEmpty()) list.removeFirst();
        }
    }


    @AggHandlerDesc(handler=LastBut_Double.class)
    public abstract Double LastBut(Double arg, int idx);
    public static class LastBut_Double
    {
        LinkedList<Double> list = new LinkedList<Double>();
        int i;
        
        public Double getAggValue() 
        { 
            if( list.isEmpty()  || (i < 0) || (i > (list.size()-1)) ) { 
                return null;
            } else {
                return list.get( ( list.size()-i-1) );
            }
        }
        public void incAggValue(Double arg, int idx) 
        { 
            list.addLast(arg);
            i = idx;  //no safety logic required here - all handled by the getAggValue() method
        }
        public void decAggValue(Double arg, int idx) 
        { 
            if(!list.isEmpty()) list.removeFirst();
        }
    }

    @AggHandlerDesc(handler=LastBut_Boolean.class)
    public abstract Boolean LastBut(Boolean arg, int idx);
    public static class LastBut_Boolean
    {
        LinkedList<Boolean> list = new LinkedList<Boolean>();
        int i;
        
        public Boolean getAggValue() 
        { 
            if( list.isEmpty()  || (i < 0) || (i > (list.size()-1)) ) { 
                return null;
            } else {
                return list.get( ( list.size()-i-1) );
            }
        }
        public void incAggValue(Boolean arg, int idx) 
        { 
            list.addLast(arg);
            i = idx;  //no safety logic required here - all handled by the getAggValue() method
        }
        public void decAggValue(Boolean arg, int idx) 
        { 
            if(!list.isEmpty()) list.removeFirst();
        }
    }

    @AggHandlerDesc(handler=LastBut_DateTime.class)
    public abstract DateTime LastBut(DateTime arg, int idx);
    public static class LastBut_DateTime
    {
        LinkedList<DateTime> list = new LinkedList<DateTime>();
        int i;
        
        public DateTime getAggValue() 
        { 
            if( list.isEmpty()  || (i < 0) || (i > (list.size()-1)) ) { 
                return null;
            } else {
                return list.get( ( list.size()-i-1) );
            }
        }
        public void incAggValue(DateTime arg, int idx) 
        { 
            list.addLast(arg);
            i = idx;  //no safety logic required here - all handled by the getAggValue() method
        }
        public void decAggValue(DateTime arg, int idx) 
        { 
            if(!list.isEmpty()) list.removeFirst();
        }
    }

   
    
    @AggHandlerDesc(handler=LastBut_Object.class)
    public abstract Object LastBut(Object arg, int idx);

    public static class LastBut_Object
    {
        LinkedList<Object> list = new LinkedList<Object>();
        // a list of say, four elements would be accessed through index 0..3
        // If I ask for FirstBut(0) then I need to specify an index of 3, effectively: "size()-'requestedIndex'-1" (= 4-0-1 =3) 
        // If I ask for FirstBut(1) then I need to specify an index of 2, effectively: "size()-'requestedIndex'-1" (= 4-1-1 =2)
        // If I ask for FirstBut(2) then I need to specify an index of 1, effectively: "size()-'requestedIndex'-1" (= 4-2-1 =1)
        // If I ask for FirstBut(3) then I need to specify an index of 0, effectively: "size()-'requestedIndex'-1" (= 4-3-1 =0)
        // If I ask for FirstBut(>3) i.e. "requestedIndex > size()-1" then I need to return null
        // If I ask for FirstBut(<0) then I need to return null
        
        int i;
        
        public Object getAggValue() 
        { 
            if( list.isEmpty()  || (i < 0) || (i > (list.size()-1)) ) { 
                return null;
            } else {
                return list.get( ( list.size()-i-1) );
            }
        }
        public void incAggValue(Object arg, int idx) 
        { 
            list.addLast(arg);
            i = idx;  //no safety logic required here - all handled by the getAggValue() method
        }
        public void decAggValue(Object arg, int idx) 
        { 
            if(!list.isEmpty()) list.removeFirst();
        }
    }

    @AggHandlerDesc(handler=StdDevPDouble.class)
    public abstract Double StdDevP(Double arg);

    // Standard Deviation, for a Population 
    // Double inputs only - recommended to cast any variable using TO_DOUBLE()  e.g.   SELECT StdDevP( TO_DOUBLE( x )) ...
    public static class StdDevPDouble
    {
        Double m = (double) 0;
        Double S = (double) 0;
        int  count = 0;

        public Double getAggValue()
        {
            Double variance = S/count;
            return (count == 0) ? 0 : Math.sqrt( variance );
        }
        public void incAggValue(Double arg)
        {
            if(arg != null) {
                Double prev_mean = m;
                count++;
                m = m + (arg-m)/count;
                S = S + (arg-m)*(arg-prev_mean);
            }
        }
        public void decAggValue(Double arg)
        {
            if(arg != null) {
                Double prev_mean = m;
                count--;
                m = m - (arg-m)/count;
                S = S - (arg-m)*(arg-prev_mean);
            }
        }
    }

    // Standard Deviation, for a Sample 
    // Double inputs only - recommended to cast any variable using TO_DOUBLE()  e.g.   SELECT StdDevS( TO_DOUBLE( x )) ...
    @AggHandlerDesc(handler=StdDevSDouble.class)
    public abstract Double StdDevS(Double arg);

    public static class StdDevSDouble
    {
        Double m = (double) 0;
        Double S = (double) 0;
        int  count = 0;

        public Double getAggValue()
        {
            Double variance = S/(count-1);
            return (count == 0) ? 0 : Math.sqrt( variance );
        }
        public void incAggValue(Double arg)
        {
            if(arg != null) {
                Double prev_mean = m;
                count++;
                m = m + (arg-m)/count;
                S = S + (arg-m)*(arg-prev_mean);
            }
        }
        public void decAggValue(Double arg)
        {
            if(arg != null) {
                Double prev_mean = m;
                count--;
                m = m - (arg-m)/count;
                S = S - (arg-m)*(arg-prev_mean);
            }
        }
    }
}
