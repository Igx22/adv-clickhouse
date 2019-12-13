package adv.clickhouse.model;

import adv.clickhouse.ChColumn;
import adv.clickhouse.ChTable;

import static adv.clickhouse.ChType.UINT16;


@ChTable("")
public class DbEventTimer {
    @ChColumn(name = "stage", type = UINT16)
    private Short stage;

    @ChColumn(name = "elapsedTime", type = UINT16)
    private Short elapsedTime;

    public DbEventTimer() {
    }

    public DbEventTimer(Short stage, Short elapsedTime) {

        this.stage = stage;
        this.elapsedTime = elapsedTime;
    }

    public Short getStage() {
        return stage;
    }

    public void setStage(Short stage) {
        this.stage = stage;
    }

    public Short getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(Short elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    @Override
    public String toString() {
        return "DbEventTimer{" +
                "stage=" + stage +
                ", elapsedTime=" + elapsedTime +
                '}';
    }
}
