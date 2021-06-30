package org.myorg.quickstart;

public class ActivityStat extends Activity {

    private final int numIpByUid;

    public ActivityStat(String eventType, String uid, String timestamp, String ip, String impressionId,
                        int numIpByUid) {
        super(eventType, uid, timestamp, ip, impressionId);
        this.numIpByUid = numIpByUid;
    }

    public int getNumIpByUid() {
        return numIpByUid;
    }

    @Override
    public String toString() {
        String s = super.toString();
        return s + " numIpByUid : " + this.numIpByUid;
    }
}
