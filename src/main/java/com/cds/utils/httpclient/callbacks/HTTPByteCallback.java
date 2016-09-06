package com.cds.utils.httpclient.callbacks;
import com.cds.utils.httpclient.SalutHttpClient;
import org.apache.http.HttpResponse;

public abstract class HTTPByteCallback extends HTTPStatusCheckCallback {

    @Override
    protected void checkedResponse(HttpResponse response) {
        this.byteResponse(SalutHttpClient.getResponseAsBytes(response), response);
    }

    protected abstract void byteResponse(byte[] body, HttpResponse response);

}
