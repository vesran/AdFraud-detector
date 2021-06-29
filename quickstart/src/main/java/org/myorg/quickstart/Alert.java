package org.myorg.quickstart;

public final class Alert extends Object {
    private String Id;
    private FraudulentPatterns alertPattern;

    public Alert(){}
    public Alert(FraudulentPatterns alertPattern){
        this.alertPattern = alertPattern;
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

    @Override
    public String toString() {
        return "Alert{" +
                " alertPattern=" + alertPattern +
                '}';
    }
}
