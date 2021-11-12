---
title: Proxy endpoint
keywords: 
last_updated: April 20, 2017
tags: []
summary: "Detailed description of how the proxy endpoint works and its configuration."
---

## Overview

This endpoint proxies an endpoint running in your local box with your app in the SLINGR platform.
It is meant for development of endpoints. See [Create your own endpoint]({{site.baseurl}}/extensions_create_your_own_endpoints.html) 
for more information.

## Configuration

### Endpoint URI

The endpoint URI is where the external service and SLINGR find your local endpoint. This must be a public URL 
(so you might need to configure port forwarding in your router) and points to your TCP port specified by 
`_webservices_port` (`TCP 10000` by default) or more convenient, use [ngrok](https://ngrok.com/) which will open
a secure tunnel to your localhost.

### Endpoint token

This is a token that will be used to verify messages coming from and to the proxy endpoint are
valid. We suggest the auto-generated token, but you can change it if you want. 

This token will be in your local endpoint configuration (see below).

### Configuration

This is the basic configuration you should use in your local endpoint. Check the SDK you are using
to know where this has to be copied.

## Javascript API

The Javascript API will be defined by your endpoint's functions and scripts. Please see
[Endpoint features]({{site.baseurl}}/extensions_common_features.html) to understand what can be done.

## Events

Events will be defined in your endpoint's configuration file. Please see 
[Endpoint features]({{site.baseurl}}/extensions_common_features.html) for more information.

## About SLINGR

SLINGR is a low-code rapid application development platform that accelerates development, with robust architecture for integrations and executing custom workflows and automation.

[More info about SLINGR](https://slingr.io)

## License

This endpoint is licensed under the Apache License 2.0. See the `LICENSE` file for more details.

