package com.bigdata.hadoop_offline;

public class Access {
    private String startTime;
    private String ip;
    private String proxyIp;
    private int responsetime;
    private String referer;
    private String method;
    private String url;
    private int httpcode;
    private int requestsize;
    private int responsesize;
    private String cache;
    private String ua;
    private String fileType;
    private String country;
    private String province;
    private String city;
    private String operator;

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
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

    public int getResponsetime() {
        return responsetime;
    }

    public void setResponsetime(int responsetime) {
        this.responsetime = responsetime;
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

    public int getHttpcode() {
        return httpcode;
    }

    public void setHttpcode(int httpcode) {
        this.httpcode = httpcode;
    }

    public int getRequestsize() {
        return requestsize;
    }

    public void setRequestsize(int requestsize) {
        this.requestsize = requestsize;
    }

    public int getResponsesize() {
        return responsesize;
    }

    public void setResponsesize(int responsesize) {
        this.responsesize = responsesize;
    }

    public String getCache() {
        return cache;
    }

    public void setCache(String cache) {
        this.cache = cache;
    }

    public String getUa() {
        return ua;
    }

    public void setUa(String ua) {
        this.ua = ua;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
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

    @Override
    public String toString() {
        return 
                startTime + ',' +
                ip + ',' +
                proxyIp + ',' +
                responsetime + ',' +
                referer + ',' +
                method + ',' +
                url + ',' +
                httpcode + ','+
                requestsize + ','+
                responsesize + ',' +
                cache + ',' +
                ua + ',' +
                fileType + ',' +
                country + ',' +
                province + ',' +
                city + ',' +
                operator ;
    }
}
