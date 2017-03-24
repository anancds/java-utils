package com.cds.utils.httpclient.callbacks;

import com.cds.utils.httpclient.SalutHttpClient;
import org.apache.http.HttpResponse;

public abstract class HTTPStringCallback extends HTTPStatusCheckCallback {

    @Override
    protected void checkedResponse(HttpResponse response) {
        this.stringResponse(SalutHttpClient.getResponseAsString(response), response);
    }

    protected abstract void stringResponse(String body, HttpResponse response);

}

