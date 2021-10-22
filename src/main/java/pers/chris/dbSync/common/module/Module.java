package pers.chris.dbSync.common.module;

import java.util.Map;

public abstract class Module {

    private String rule;

    public Module(String rule) {
        this.rule = rule;
    }

    public abstract void run(Map<String, String> data);

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

}
