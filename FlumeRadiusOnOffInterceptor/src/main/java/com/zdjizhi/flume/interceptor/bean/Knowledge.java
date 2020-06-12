package com.zdjizhi.flume.interceptor.bean;

/**
 * @author qidaijie
 */
public class Knowledge {
    private String framed_ip;
    private String account;
    private String acct_session_id;
    private int acct_status_type;
    private int acct_session_time;
    private long event_timestamp;

    public String getFramed_ip() {
        return framed_ip;
    }

    public void setFramed_ip(String framed_ip) {
        this.framed_ip = framed_ip;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public int getAcct_status_type() {
        return acct_status_type;
    }

    public void setAcct_status_type(int acct_status_type) {
        this.acct_status_type = acct_status_type;
    }

    public long getEvent_timestamp() {
        return event_timestamp;
    }

    public void setEvent_timestamp(long event_timestamp) {
        this.event_timestamp = event_timestamp;
    }

    public String getAcct_session_id() {
        return acct_session_id;
    }

    public void setAcct_session_id(String acct_session_id) {
        this.acct_session_id = acct_session_id;
    }

    public int getAcct_session_time() {
        return acct_session_time;
    }

    public void setAcct_session_time(int acct_session_time) {
        this.acct_session_time = acct_session_time;
    }
}
