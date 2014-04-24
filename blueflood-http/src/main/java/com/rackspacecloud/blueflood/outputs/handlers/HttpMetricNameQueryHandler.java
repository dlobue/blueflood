/**
 * Copyright 2014 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.outputs.handlers;

import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;
import org.json.simple.JSONObject;

public class HttpMetricNameQueryHandler implements HttpRequestHandler {

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        final String tenantId = request.getHeader("tenantId");
        final String metricName = request.getHeader("metricName");

        if (!(request instanceof HTTPRequestWithDecodedQueryParams)) {
            sendResponse(ctx, request, "Missing query params: from, to, points",
                    HttpResponseStatus.BAD_REQUEST);
            return;
        }

        HTTPRequestWithDecodedQueryParams requestWithParams = (HTTPRequestWithDecodedQueryParams) request;

        final TimerContext httpMetricsFetchTimerContext = httpMetricsFetchTimer.time();
        try {
            RollupsQueryParams params = PlotRequestParser.parseParams(requestWithParams.getQueryParams());

            JSONObject metricData;
            if (params.isGetByPoints()) {
                metricData = GetDataByPoints(tenantId, metricName, params.getRange().getStart(),
                        params.getRange().getStop(), params.getPoints(), params.getStats());
            } else if (params.isGetByResolution()) {
                metricData = GetDataByResolution(tenantId, metricName, params.getRange().getStart(),
                        params.getRange().getStop(), params.getResolution(), params.getStats());
            } else {
                throw new InvalidRequestException("Invalid rollups query. Neither points nor resolution specified.");
            }

            final JsonElement element = parser.parse(metricData.toString());
            final String jsonStringRep = gson.toJson(element);
            sendResponse(ctx, request, jsonStringRep, HttpResponseStatus.OK);
        } catch (InvalidRequestException e) {
            log.error(e.getMessage(), e);
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        } catch (SerializationException e) {
            log.error(e.getMessage(), e);
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            sendResponse(ctx, request, e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            httpMetricsFetchTimerContext.stop();
        }
    }

    private void sendResponse(ChannelHandlerContext channel, HttpRequest request, String messageBody,
                              HttpResponseStatus status) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);

        if (messageBody != null && !messageBody.isEmpty()) {
            response.setContent(ChannelBuffers.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
        }
        HttpResponder.respond(channel, request, response);
    }
}
