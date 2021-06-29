package org.myorg.quickstart;

public final class Alert extends Activity {
    private String Id;
    private FraudulentPatterns alertPattern;

    public Integer getCountElement() {
        return countElement;
    }

    public void setCountElement(Integer countElement) {
        this.countElement = countElement;
    }

    private Integer countElement;

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
