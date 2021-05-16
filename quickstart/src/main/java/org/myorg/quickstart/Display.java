package org.myorg.quickstart;

public class Display extends Activity{

    @Override
    public String toString() {
        StringBuilder strb = new StringBuilder();
        strb.append("display");
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
