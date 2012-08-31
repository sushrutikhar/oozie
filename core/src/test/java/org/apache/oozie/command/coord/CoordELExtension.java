package org.apache.oozie.command.coord;

import java.util.Calendar;

import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.util.ELEvaluator;

public class CoordELExtension {
    private static final String PREFIX = "coordext:";

    private enum TruncateBoundary {
        NONE, DAY, MONTH, QUARTER, YEAR;
    }

    public static String ph1_today_echo(int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return PREFIX + "today(" + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph1_currentMonth_echo(int day, int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return PREFIX + "currentMonth(" + day + ", " + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph2_today_inst(int hr, int min) throws Exception {
        return mapToCurrentInstance(TruncateBoundary.DAY, 0, 0, 0, hr, min);
    }

    public static String ph2_currentMonth_inst(int day, int hr, int min) throws Exception {
        return mapToCurrentInstance(TruncateBoundary.MONTH, 0, 0, day, hr, min);
    }

    public static String ph2_today(int hr, int min) throws Exception {
        String inst = ph2_today_inst(hr, min);
        return evaluateCurrent(inst);
    }

    public static String ph2_currentMonth(int day, int hr, int min) throws Exception {
        String inst = ph2_currentMonth_inst(day, hr, min);
        return evaluateCurrent(inst);
    }
    
    private static String evaluateCurrent(String curExpr) throws Exception {
        if(curExpr.equals("")) {
            return curExpr;
        }

        int inst = CoordCommandUtils.parseOneArg(curExpr);
        return CoordELFunctions.ph2_coord_current(inst);
    }

    /**
     * Maps the dataset time to coord:current(n) with respect to action's nominal time
     * dataset time = truncate(nominal time) + yr + day + month + hr + min
     * @param truncField
     * @param yr
     * @param month
     * @param day
     * @param hr
     * @param min
     * @return coord:current(n)
     * @throws Exception
     */
    //TODO handle the case where action_Creation_time or the n-th instance is earlier than the Initial_Instance of dataset.
    private static String mapToCurrentInstance(TruncateBoundary trunc, int yr, int month, int day, int hr, int min) throws Exception {
        Calendar nominalInstanceCal = CoordELFunctions.getEffectiveNominalTime();
        if (nominalInstanceCal == null) {
            return "";
        }

        Calendar dsInstanceCal = Calendar.getInstance(CoordELFunctions.getDatasetTZ());
        dsInstanceCal.setTime(nominalInstanceCal.getTime());

        //truncate
        switch (trunc) {
            case YEAR:
                dsInstanceCal.set(Calendar.MONTH, 0);
            case MONTH:
                dsInstanceCal.set(Calendar.DAY_OF_MONTH, 1);
            case DAY:
                dsInstanceCal.set(Calendar.HOUR_OF_DAY, 0);
                dsInstanceCal.set(Calendar.MINUTE, 0);
                dsInstanceCal.set(Calendar.SECOND, 0);
                dsInstanceCal.set(Calendar.MILLISECOND, 0);
                break;
            case NONE:    //don't truncate
                break;
            default:
                throw new IllegalArgumentException("Truncation boundary " + trunc + " is not supported");
        }

        //add
        dsInstanceCal.add(Calendar.YEAR, yr);
        dsInstanceCal.add(Calendar.MONTH, month);
        dsInstanceCal.add(Calendar.DAY_OF_MONTH, day);
        dsInstanceCal.add(Calendar.HOUR_OF_DAY, hr);
        dsInstanceCal.add(Calendar.MINUTE, min);

        int[] instCnt = new int[1];
        Calendar compInstCal = CoordELFunctions.getCurrentInstance(dsInstanceCal.getTime(), instCnt);
        if(compInstCal == null) {
            return "";
        }
        int dsInstanceCnt = instCnt[0];

        compInstCal = CoordELFunctions.getCurrentInstance(nominalInstanceCal.getTime(), instCnt);
        if(compInstCal == null) {
            return "";
        }
        int nominalInstanceCnt = instCnt[0];

        return "coord:current(" + (dsInstanceCnt - nominalInstanceCnt) + ")";
    }
}
