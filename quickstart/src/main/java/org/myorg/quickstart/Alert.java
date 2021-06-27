package org.myorg.quickstart;

public final class Alert extends Object {
    private String Id;
    private float uidClickPerDisplayRatio;
    private FraudulentPatterns alertPattern;

    public Alert(){}
    public Alert(FraudulentPatterns alertPattern){
        this.alertPattern = alertPattern;
    }


    public float getUidClickPerDisplayRatio() {
        return uidClickPerDisplayRatio;
    }

    public void setUidClickPerDisplayRatio(float uidClickPerDisplayRatio) {
        this.uidClickPerDisplayRatio = uidClickPerDisplayRatio;
    }

    public String getId() {
        return this.Id;
    }
    public void setId(String uid) {
        this.Id = uid;
    }

    public FraudulentPatterns getAlertPattern() {
        return alertPattern;
    }
}
