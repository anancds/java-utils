package com.cds.utils.httpclient;

import java.io.IOException;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.util.EntityUtils;

public final class SalutHttpClient {

    private SalutHttpClient() {
        //
    }

    /**
     * @param url the base URL
     * @return the created {@link HTTPRequest}
     */
    public static HTTPRequest url(final String url) {
        return new HTTPRequest(url);
    }

    /**
     * @param response the {@link HttpResponse}
     * @return String the body as UTF-8 string
     */
    public static String getResponseAsString(final HttpResponse response) {
        try {
            return EntityUtils.toString(response.getEntity(), "UTF-8");
        } catch (final ParseException e) {
            throw new RuntimeException(e);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param response the {@link HttpResponse}
     * @return String the body as UTF-8 string
     */
    public static byte[] getResponseAsBytes(final HttpResponse response) {
        try {
            return EntityUtils.toByteArray(response.getEntity());
        } catch (final ParseException e) {
            throw new RuntimeException(e);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param response Response
     * @return String
     */
    public static int getStatus(final HttpResponse response) {
        return response.getStatusLine().getStatusCode();
    }

    /**
     * @param response Response
     * @return true if status code if between 200 and 299
     */
    public static boolean isStatusOK(final HttpResponse response) {
        final int code = SalutHttpClient.getStatus(response);
        return (code >= 200) && (code <= 299);
    }

    /**
     * @param response Response
     * @return true if status code if between 300 and 399
     */
    public static boolean isStatusRedirect(final HttpResponse response) {
        final int code = SalutHttpClient.getStatus(response);
        return (code >= 300) && (code <= 399);
    }

    /**
     * @param response Response
     * @return true if status code if between 400 and 499
     */
    public static boolean isStatusClientError(final HttpResponse response) {
        final int code = SalutHttpClient.getStatus(response);
        return (code >= 400) && (code <= 499);
    }

    /**
     * @param response Response
     * @return true if status code if between 500 and 599
     */
    public static boolean isStatusServerError(final HttpResponse response) {
        final int code = SalutHttpClient.getStatus(response);
        return (code >= 500) && (code <= 599);
    }

    public static void main(String[] args) {
        HttpResponse response = SalutHttpClient.url("http://www.baidu.com").post();
        System.out.println(SalutHttpClient.getResponseAsString(response));
    }
}
