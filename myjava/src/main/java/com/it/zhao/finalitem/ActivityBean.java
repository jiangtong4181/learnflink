package com.it.zhao.finalitem;

public class ActivityBean {
    public ActivityBean(String uid, String aid, String times, String eid) {
        this.uid = uid;
        this.aid = aid;
        this.times = times;
        this.eid = eid;
    }

    public String uid;
    public String aid;
    public String times;
    public String eid;
    public Long activityCount;
    public Long activityDisCount;


    @Override
    public String toString() {
        return "ActivityBean{" +
                "uid='" + uid + '\'' +
                ", aid='" + aid + '\'' +
                ", times='" + times + '\'' +
                ", eid='" + eid + '\'' +
                ", activityCount=" + activityCount +
                ", activityDisCount=" + activityDisCount +
                '}';
    }

    public static ActivityBean of(String uid, String aid, String times, String eid){
        return new ActivityBean(uid,aid,times,eid);
    }
}
