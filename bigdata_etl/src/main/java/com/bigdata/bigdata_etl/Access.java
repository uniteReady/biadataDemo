package com.bigdata.bigdata_etl;

public class Access {
//    private String startTime;
    private String ip;
    private String proxyIp;
    private long responseTime;
    private String referer;
    private String method;
    private String url;
    private long httpCode;
    private long requestSize;
    private long responseSize;
    private String cache;

//    private String ua;
//    private String fileType;

//    private String country;
    private String province;
    private String city;
    private String operator;

    private String http;
    private String domain;
    private String path;

    private String year;
    private String month;
    private String day;

    public String getHttp() {
        return http;
    }

    public void setHttp(String http) {
        this.http = http;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }



    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getProxyIp() {
        return proxyIp;
    }

    public void setProxyIp(String proxyIp) {
        this.proxyIp = proxyIp;
    }

    public long getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(long responseTime) {
        this.responseTime = responseTime;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getHttpCode() {
        return httpCode;
    }

    public void setHttpCode(long httpCode) {
        this.httpCode = httpCode;
    }

    public long getRequestSize() {
        return requestSize;
    }

    public void setRequestSize(long requestSize) {
        this.requestSize = requestSize;
    }

    public long getResponseSize() {
        return responseSize;
    }

    public void setResponseSize(long responseSize) {
        this.responseSize = responseSize;
    }

    public String getCache() {
        return cache;
    }

    public void setCache(String cache) {
        this.cache = cache;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    @Override
    public String toString() {
        return 

                ip + ',' +
                proxyIp + ',' +
                responseTime + ',' +
                referer + ',' +
                method + ',' +
                url + ',' +
                httpCode + ','+
                requestSize + ','+
                responseSize + ',' +
                cache + ',' +
                province + ',' +
                city + ',' +
                operator  + ',' +
                http  + ',' +
                domain  + ',' +
                path  + ',' +
                year + ',' +
                month  + ',' +
                day;
    }
}
