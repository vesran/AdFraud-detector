package org.myorg.quickstart;

public class ActivityStat extends Activity {

    private int numIpByUid;
    private double reactionTime;
    private int numClicks;

    public ActivityStat(String eventType, String uid, String timestamp, String ip, String impressionId) {
        super(eventType, uid, timestamp, ip, impressionId);
    }

    public int getNumIpByUid() {
        return numIpByUid;
    }

    public void setNumIpByUid(int num) { this.numIpByUid = num;}

    public void setReactionTime(double reactionTime) {
        this.reactionTime = reactionTime;
    }

    public void setNumClicks(int numClicks) {
        this.numClicks = numClicks;
    }

    public double getReactionTime() {
        return reactionTime;
    }

    public int getNumClicks() {
        return numClicks;
    }

    @Override
    public String toString() {
        String s = super.toString();
        return s + " numIpByUid : " + this.numIpByUid
                + " reaction time : " + this.reactionTime
                + " num clicks : " + this.numClicks;
    }
}
