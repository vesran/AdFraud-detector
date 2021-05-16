package org.myorg.quickstart;

public class Click extends Activity {

    @Override
    public String toString() {
        StringBuilder strb = new StringBuilder();
        strb.append("click");
        strb.append(" -> ");
        strb.append(" uid : ");
        strb.append(this.uid);
        strb.append(", timestamp : ");
        strb.append(this.getTimestampAsDate());
        strb.append(", ip : ");
        strb.append(this.ip);
        strb.append(", impressionId : ");
        strb.append(this.impressionId);

        return strb.toString();
    }
}
