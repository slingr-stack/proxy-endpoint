package io.slingr.endpoints.proxy;

import io.slingr.endpoints.Endpoint;
import io.slingr.endpoints.exceptions.EndpointException;
import io.slingr.endpoints.exceptions.ErrorCode;
import io.slingr.endpoints.framework.annotations.*;
import io.slingr.endpoints.services.AppLogs;
import io.slingr.endpoints.services.datastores.DataStore;
import io.slingr.endpoints.services.datastores.DataStoreResponse;
import io.slingr.endpoints.services.exchange.ApiUri;
import io.slingr.endpoints.services.exchange.Parameter;
import io.slingr.endpoints.services.logs.AppLogLevel;
import io.slingr.endpoints.services.rest.DownloadedFile;
import io.slingr.endpoints.services.rest.RestClient;
import io.slingr.endpoints.services.rest.RestClientBuilder;
import io.slingr.endpoints.services.rest.RestMethod;
import io.slingr.endpoints.utils.FilesUtils;
import io.slingr.endpoints.utils.Json;
import io.slingr.endpoints.utils.Strings;
import io.slingr.endpoints.ws.exchange.FunctionRequest;
import io.slingr.endpoints.ws.exchange.UploadedFile;
import io.slingr.endpoints.ws.exchange.WebServiceRequest;
import io.slingr.endpoints.ws.exchange.WebServiceResponse;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Endpoint used as proxy to endpoints on the developer environment
 *
 * Created by lefunes on 17/05/17.
 */
@SlingrEndpoint(name = "proxy")
public class ProxyEndpoint extends Endpoint {
    private static final Logger logger = LoggerFactory.getLogger(ProxyEndpoint.class);
    @ApplicationLogger
    private AppLogs appLogs;

    private static final String DATA_STORE_NAME = "__ds_name__";
    private static final String DATA_STORE_NEW_ID = "__ds_id__";
    private static final String DATA_STORE_ID = "_id";
    private static final String CONFIGURATION_HELP_URL_VALUE = "/endpoints_proxy.html#configuration";

    // endpoint services uris
    private static final String VAR_KEY = "key";
    private static final String VAR_DATA_STORE = "dataStore";
    private static final String VAR_DOCUMENT_ID = "documentId";
    private static final String VAR_FILE_ID = "fileId";
    private static final String URL_CONFIGURATION =     ApiUri.ES_URL_PREFIX+ApiUri.ES_URL_CONFIGURATION;
    private static final String URL_ASYNC_EVENT =       ApiUri.ES_URL_PREFIX+ApiUri.ES_URL_ASYNC_EVENT;
    private static final String URL_SYNC_EVENT =        ApiUri.ES_URL_PREFIX+ApiUri.ES_URL_SYNC_EVENT;
    private static final String URL_CONFIG_SCRIPT =     ApiUri.ES_URL_PREFIX+ApiUri.ES_URL_CONFIG_SCRIPT;
    private static final String URL_APP_LOG =           ApiUri.ES_URL_PREFIX+ApiUri.ES_URL_APP_LOG;
    private static final String URL_FILE_UPLOAD =       ApiUri.ES_URL_PREFIX+ApiUri.ES_URL_FILE_UPLOAD;
    private static final String URL_LOCK =              ApiUri.ES_URL_PREFIX+ApiUri.ES_URL_ENDPOINTS_PREFIX+ApiUri.ES_PART_LOCK+"/{"+VAR_KEY+"}";
    private static final String URL_FILE_DOWNLOAD =     ApiUri.ES_URL_PREFIX+ApiUri.ES_URL_ENDPOINTS_PREFIX+ApiUri.ES_PART_FILE+"/{"+VAR_FILE_ID+"}";
    private static final String URL_FILE_METADATA =     ApiUri.ES_URL_PREFIX+ApiUri.ES_URL_ENDPOINTS_PREFIX+ApiUri.ES_PART_FILE+"/{"+VAR_FILE_ID+"}/"+ApiUri.ES_PART_METADATA;
    private static final String URL_DATA_STORE =        ApiUri.ES_URL_PREFIX+ApiUri.ES_URL_ENDPOINTS_PREFIX+ApiUri.ES_PART_DATA_STORE+"/{"+VAR_DATA_STORE+"}";
    private static final String URL_DATA_STORE_BY_ID =  ApiUri.ES_URL_PREFIX+ApiUri.ES_URL_ENDPOINTS_PREFIX+ApiUri.ES_PART_DATA_STORE+"/{"+VAR_DATA_STORE+"}/{"+VAR_DOCUMENT_ID+"}";
    private static final String URL_DATA_STORE_COUNT =  ApiUri.ES_URL_PREFIX+ApiUri.ES_URL_ENDPOINTS_PREFIX+ApiUri.ES_PART_DATA_STORE+"/{"+VAR_DATA_STORE+"}/"+ApiUri.ES_PART_COUNT;
    private static final String URL_CLEAR_CACHE =       ApiUri.ES_URL_PREFIX+ApiUri.ES_URL_CLEAR_CACHE;

    @ApplicationLogger
    private AppLogs appLogger;

    @EndpointProperty
    private String endpointUri;

    @EndpointProperty
    private String endpointToken;

    @EndpointDataStore(name = "ds")
    private DataStore dataStore;

    @Override
    public void webServicesConfigured() {
        // enable interceptors
        baseModule.enableConfiguratorInterceptor();
        baseModule.enableFunctionInterceptor();
        baseModule.enableWebServicesInterceptor();
    }

    @Override
    public void endpointStarted() {
        logger.info(String.format("Configured Proxy endpoint - Endpoint URI [%s], Endpoint Token [%s]", endpointUri, Strings.maskToken(endpointToken)));
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Endpoints API
    ///////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public Object functionInterceptor(FunctionRequest request) throws EndpointException {
        final String functionName = request.getFunctionName();
        logger.info(String.format("Function request [%s] - id [%s]", functionName, request.getFunctionId()));
        appLogs.info(String.format("Function request [%s] - id [%s]", functionName, request.getFunctionId()));

        final Json jsonRequest = request.toJson().set(Parameter.FUNCTION_NAME, functionName);

        try {
            Object body = request.getParams();
            appLogs.info(body.toString());
            if(!(body instanceof Json || body instanceof Map || body instanceof List)){
                body = Json.map().set(Parameter.REQUEST_WRAPPED, body);
            }
            appLogs.info(body.toString());
            jsonRequest.set(Parameter.PARAMS, body);

            final Json response = postJsonFromEndpoint(ApiUri.URL_FUNCTION, jsonRequest);

            logger.info(String.format("Function response [%s] received - id [%s]", functionName, request.getFunctionId()));
            appLogs.info(String.format("Function response [%s] received - id [%s]", functionName, request.getFunctionId()));
            return response.contains(Parameter.DATA) ? response.json(Parameter.DATA) : Json.map();
        } catch (EndpointException ex){
            appLogger.error(String.format("Exception when try to execute function on endpoint: %s", ex.toString()));
            throw ex;
        } catch (Exception ex){
            appLogger.error(String.format("Exception when try to execute function on endpoint: %s", ex.getMessage()));
            throw EndpointException.permanent(ErrorCode.CLIENT, String.format("Exception when try to execute function on endpoint: %s", ex.getMessage()), ex);
        }
    }

    @Override
    public Json configurationInterceptor(Json configuration) throws EndpointException {
        Json response = configuration;
        try {
            // removes proxy configuration
            response.set(Parameter.METADATA_HELP_URL, CONFIGURATION_HELP_URL_VALUE)
                    .remove(Parameter.METADATA_PER_USER)
                    .remove(Parameter.METADATA_CONFIGURATION)
                    .remove(Parameter.METADATA_FUNCTIONS)
                    .remove(Parameter.METADATA_EVENTS)
                    .remove(Parameter.METADATA_USER_CONF)
                    .remove(Parameter.METADATA_USER_CONF_BUTTONS)
                    .remove(Parameter.METADATA_JS)
                    .remove(Parameter.METADATA_LISTENERS);

            // get configuration from the external endpoint
            final Json endpointConfiguration = getJsonFromEndpoint(ApiUri.URL_CONFIGURATION);
            if(endpointConfiguration != null) {
                logger.info("Properties received from endpoint");
                response.set(Parameter.METADATA_PER_USER, endpointConfiguration.is(Parameter.METADATA_PER_USER, false))
                        .setIfNotNull(Parameter.METADATA_CONFIGURATION, endpointConfiguration.json(Parameter.METADATA_CONFIGURATION))
                        .setIfNotNull(Parameter.METADATA_FUNCTIONS, endpointConfiguration.jsons(Parameter.METADATA_FUNCTIONS))
                        .setIfNotNull(Parameter.METADATA_EVENTS, endpointConfiguration.jsons(Parameter.METADATA_EVENTS))
                        .setIfNotEmpty(Parameter.METADATA_USER_CONF, endpointConfiguration.jsons(Parameter.METADATA_USER_CONF))
                        .setIfNotEmpty(Parameter.METADATA_USER_CONF_BUTTONS, endpointConfiguration.json(Parameter.METADATA_USER_CONF_BUTTONS))
                        .setIfNotEmpty(Parameter.METADATA_JS,endpointConfiguration.string(Parameter.METADATA_JS))
                        .setIfNotEmpty(Parameter.METADATA_LISTENERS, endpointConfiguration.string(Parameter.METADATA_LISTENERS));
            }
        } catch (EndpointException ex){
            appLogger.error(String.format("Exception when try to request configuration from endpoint: %s", ex.getMessage()));
            logger.warn(String.format("Exception when try to request configuration from endpoint: %s", ex.toString()));
        } catch (Exception ex){
            appLogger.error(String.format("Exception when try to request configuration from endpoint: %s", ex.getMessage()));
            logger.warn(String.format("Exception when try to request configuration from endpoint: %s", ex.getMessage()), ex);
        }
        return response;
    }

    private Json getJsonFromEndpoint(final String path) {
        return RestClient.builder(this.endpointUri)
                .header(Parameter.TOKEN, this.endpointToken)
                .path(path)
                .get();
    }

    private Json postJsonFromEndpoint(final String path, final Json content) {
        return RestClient.builder(this.endpointUri)
                .header(Parameter.TOKEN, this.endpointToken)
                .path(path)
                .post(content);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Endpoint Web Services
    ///////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public Object webServicesInterceptor(WebServiceRequest request) throws EndpointException {
        if(StringUtils.isBlank(this.endpointUri)){
            return null;
        }

        String path = request.getPath();
        if(StringUtils.isBlank(path)){
            path = "/";
        }
        path = path.trim();

        final String queryString = Strings.convertToQueryString(request.getParameters());

        final Object body = request.getBody();
        final String bodyLog = request.getMethod() == RestMethod.POST || request.getMethod() == RestMethod.PUT || request.getMethod() == RestMethod.PATCH ?
                String.format(" - body [%s]", body != null ? body : "-") : "";
        logger.info(String.format("Generic web service request [%s %s%s]%s", request.getMethod().toString(), path, StringUtils.isNotBlank(queryString) ? String.format("?%s", queryString) : "", bodyLog));
        appLogs.info(String.format("Generic web service request [%s %s%s]%s", request.getMethod().toString(), path, StringUtils.isNotBlank(queryString) ? String.format("?%s", queryString) : "", bodyLog));

        final RestClientBuilder client = RestClient
                .builder(this.endpointUri)
                .path(path);

        request.getHeaders().forEachMap((key, value) -> {
            if(!Parameter.CONTENT_LENGTH.equalsIgnoreCase(key) && !Parameter.HOST.equalsIgnoreCase(key)) {
                client.header(key, value);
            }
        });
        request.getParameters().forEachMapString(client::parameter);

        final Json endpointResponse;
        switch (request.getMethod()){
            case GET:
                endpointResponse = client.get(true);
                break;
            case POST:
                endpointResponse = client.post(body, true);
                break;
            case PUT:
                endpointResponse = client.put(body, true);
                break;
            case DELETE:
                endpointResponse = client.delete(true);
                break;
            case OPTIONS:
                endpointResponse = client.options(true);
                break;
            case HEAD:
                endpointResponse = client.head(true);
                break;
            case PATCH:
                endpointResponse = client.patch(body, true);
                break;
            default:
                endpointResponse = client.get(true);
        }

        final WebServiceResponse response;
        if(endpointResponse == null){
            response = new WebServiceResponse(String.format("Invalid response to [%s] method: no response", request.getMethod()));
            response.setHttpCode(500);
        } else if(!endpointResponse.contains("body")){
            response = new WebServiceResponse(String.format("Invalid response to [%s] method: %s", request.getMethod(), endpointResponse));
            response.setHttpCode(500);
        } else {
            response = new WebServiceResponse(endpointResponse.object("body"));
            appLogs.info(endpointResponse.object("body").toString());
            appLogs.info(response.toString());
            if(endpointResponse.contains("status")){
                try {
                    response.setHttpCode(endpointResponse.integer("status"));
                } catch (Exception ex){
                    logger.warn(String.format("Exception on received status code [%s] - code 200 is returned: %s", endpointResponse.object("status"), ex.getMessage()), ex);
                }
            }
            if(endpointResponse.contains("headers")){
                try {
                    final Json hd = endpointResponse.json("headers");
                    if(hd != null && hd.isNotEmpty()){
                        appLogs.info(hd.toPrettyString());
                        for (String header : hd.keys()) {
                            appLogs.info(header);
                            if(!Parameter.CONTENT_LENGTH.equals(header) && !Parameter.HOST.equalsIgnoreCase(header)) {
                                response.setHeader(header, hd.string(header));
                            }
                        }
                    }
                } catch (Exception ex){
                    logger.warn(String.format("Exception on received headers [%s] - empty headers will be returned: %s", endpointResponse.object("headers"), ex.getMessage()), ex);
                }
            }
        }
        appLogs.info(response.toString());
        return response;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Endpoints Services API
    ///////////////////////////////////////////////////////////////////////////////////////////////

    @EndpointWebService(path = URL_CONFIGURATION, methods = RestMethod.GET) 
    public Json endpointServicesEndpointConfiguration(WebServiceRequest request){
        logger.info("Properties request received");
        checkToken(request.getHeader(Parameter.TOKEN));

        return Json.map()
                .set(Parameter.CONFIGURATION_PROXY, true)
                .set(Parameter.CONFIGURATION_WEB_SERVICE_URI, properties().getWebServicesUri())
        ;
    }

    @EndpointWebService(path = URL_ASYNC_EVENT, methods = RestMethod.POST) 
    public Json endpointServicesEndpointAsyncEvent(WebServiceRequest request){
        logger.info("Event received");
        checkToken(request.getHeader(Parameter.TOKEN));

        final Json event = request.getJsonBody();

        events().send(
                event.longInteger(Parameter.DATE),
                event.string(Parameter.EVENT_NAME),
                event.object(Parameter.DATA),
                event.string(Parameter.FROM_FUNCTION_ID),
                event.string(Parameter.USER_ID),
                event.string(Parameter.USER_EMAIL),
                0
        );
        logger.info("Event sent to application");
        return Json.map();
    }

    @EndpointWebService(path = URL_SYNC_EVENT, methods = RestMethod.POST) 
    public Json endpointServicesEndpointSyncEvent(WebServiceRequest request){
        logger.info("Event received");
        checkToken(request.getHeader(Parameter.TOKEN));

        final Json event = request.getJsonBody();
        try {
            Object response = events().sendSync(
                    event.longInteger(Parameter.DATE),
                    event.string(Parameter.EVENT_NAME),
                    event.object(Parameter.DATA),
                    event.string(Parameter.FROM_FUNCTION_ID),
                    event.string(Parameter.USER_ID),
                    event.string(Parameter.USER_EMAIL),

                    0
            );
            logger.info(String.format("Sync event sent to application [%s]", response != null ? response.toString() : "-"));

            if (response == null) {
                response = Json.map();
            }

            Json jsonResponse = Json.fromObject(response, false, true);
            if (jsonResponse == null) {
                jsonResponse = Json.map().set(Parameter.SYNC_RESPONSE, response);
            }
            return jsonResponse;
        } catch (Exception ex){
            logger.warn(String.format("Exception when send sync event to application [%s]", ex.toString()));
            return Json.map().set(Parameter.SYNC_ERROR_RESPONSE, ex.getMessage());
        }
    }

    @EndpointWebService(path = URL_CONFIG_SCRIPT, methods = RestMethod.POST)
    public Json endpointServicesEndpointConfigScripts(WebServiceRequest request){
        logger.info("Config script execution request received");
        checkToken(request.getHeader(Parameter.TOKEN));

        final Json confScript = request.getJsonBody();
        try {
            Object response = scripts().execute(
                    confScript.longInteger(Parameter.DATE),
                    confScript.string(Parameter.CONFIG_SCRIPT_NAME),
                    confScript.object(Parameter.CONFIG_SCRIPT_PARAMS),
                    0
            );
            logger.info(String.format("Config script execution request sent to application [%s]", response != null ? response.toString() : "-"));

            if (response == null) {
                response = Json.map();
            }

            Json jsonResponse = Json.fromObject(response, false, true);
            if (jsonResponse == null) {
                jsonResponse = Json.map().set(Parameter.SYNC_RESPONSE, response);
            }
            return jsonResponse;
        } catch (Exception ex){
            logger.warn(String.format("Exception when send config script execution request to application [%s]", ex.toString()));
            return Json.map().set(Parameter.SYNC_ERROR_RESPONSE, ex.getMessage());
        }
    }

    @EndpointWebService(path = URL_APP_LOG, methods = RestMethod.POST) 
    public void endpointServicesEndpointAppLog(WebServiceRequest request){
        logger.info("App log received");
        checkToken(request.getHeader(Parameter.TOKEN));

        final Json appLog = request.getJsonBody();

        appLogs().sendAppLog(
                appLog.longInteger(Parameter.DATE),
                AppLogLevel.fromString(appLog.string(Parameter.APP_LOG_LEVEL)),
                appLog.string(Parameter.APP_LOG_MESSAGE),
                appLog.json(Parameter.APP_LOG_ADDITIONAL_INFO)
        );
        logger.info("App log sent to application");
    }

    @EndpointWebService(path = URL_FILE_METADATA, methods = RestMethod.GET) 
    public Json endpointServicesEndpointFileMetadata(WebServiceRequest request){
        logger.info("File - get file metadata");
        checkToken(request.getHeader(Parameter.TOKEN));

        final String fileId = request.getPathVariable(VAR_FILE_ID);
        final Json response = files().metadata(fileId);

        logger.info(String.format("File - metadata [%s]", response.string("fileName")));
        return response;
    }

    @EndpointWebService(path = URL_FILE_DOWNLOAD, methods = RestMethod.GET) 
    public InputStream endpointServicesEndpointDownloadFile(WebServiceRequest request){
        logger.info("File - download file from app");
        checkToken(request.getHeader(Parameter.TOKEN));

        final String fileId = request.getPathVariable(VAR_FILE_ID);

        final DownloadedFile file = files().download(fileId);
        if(file != null && file.getFile() != null) {
            final File tmp = FilesUtils.copyInputStreamToTemporaryFile(fileId, file.getFile(), true);
            if(tmp.exists()){
                try{
                    logger.info("File - input stream sent");
                    return new FileInputStream(tmp);
                } catch (Exception ex){
                    logger.warn(String.format("Exception when download file: %s", ex.getMessage()), ex);
                }
            }
        }
        logger.warn(String.format("File [%s] was not downloaded", fileId));
        return null;
    }

    @EndpointWebService(path = URL_FILE_UPLOAD, methods = RestMethod.POST)
    public Json endpointServicesEndpointUploadFile(WebServiceRequest request){
        logger.info("File - upload file to app");
        checkToken(request.getHeader(Parameter.TOKEN));

        InputStream fileIs = null;
        String fileName = Parameter.FILE_UPLOAD_PARAMETER;
        String fileContentType = null;

        for (UploadedFile file : request.getFiles()) {
            if(file.getName().equals(Parameter.FILE_UPLOAD_PARAMETER)){
                fileIs = file.getFile();
                fileName = file.getFilename();

                if(StringUtils.isNotBlank(file.getContentType())){
                    fileContentType = file.getContentType();
                } else {
                    fileContentType = file.getHeaders().string(Parameter.CONTENT_TYPE.toLowerCase());
                }
            }
        }

        if(fileIs == null){
            Object body = request.getBody();
            if(body instanceof String) {
                try {
                    fileIs = new ByteArrayInputStream(((String) body).getBytes(StandardCharsets.ISO_8859_1.name()));
                } catch (Exception ex) {
                    logger.warn(String.format("Exception when try to parse the file as stream: %s", ex.getMessage()), ex);
                }
            } else if(body instanceof InputStream){
                fileIs = (InputStream) body;
            }
        }

        Json response = Json.map();
        if(fileIs != null) {
            try {
                final File tmp = FilesUtils.copyInputStreamToTemporaryFile(fileName, fileIs, true);
                if (tmp.exists()) {
                    response = files().upload(fileName, new FileInputStream(tmp), fileContentType);
                } else {
                    logger.warn("Temporal file was not created");
                }
            } catch (Exception ex) {
                logger.warn(String.format("Exception when try to upload file to application: %s", ex.getMessage()), ex);
            }
        }

        if(response != null) {
            logger.info(String.format("File - file [%s]", response.string("fileId")));
        } else {
            logger.warn("Uploaded file can not be processed");
        }
        return response;
    }

    @EndpointWebService(path = URL_DATA_STORE, methods = RestMethod.POST) 
    public Json endpointServicesEndpointDataStoreSaveDocument(WebServiceRequest request){
        logger.info("Data store - save received");
        checkToken(request.getHeader(Parameter.TOKEN));

        final Json document = request.getJsonBody();
        final String dataStoreName = request.getPathVariable(VAR_DATA_STORE);

        return internalDataStoreSaveDocument("saved", dataStoreName, null, document);
    }

    @EndpointWebService(path = URL_DATA_STORE_BY_ID, methods = RestMethod.PUT)
    public Json endpointServicesEndpointDataStoreUpdateDocument(WebServiceRequest request){
        logger.info("Data store - update received");
        checkToken(request.getHeader(Parameter.TOKEN));

        final Json document = request.getJsonBody();
        final String dataStoreName = request.getPathVariable(VAR_DATA_STORE);
        final String documentId = request.getPathVariable(VAR_DOCUMENT_ID);

        return internalDataStoreSaveDocument("updated", dataStoreName, documentId, document);
    }

    @EndpointWebService(path = URL_DATA_STORE_COUNT, methods = RestMethod.GET) 
    public Json endpointServicesEndpointDataStoreCountDocuments(WebServiceRequest request){
        logger.info("Data store - count");
        checkToken(request.getHeader(Parameter.TOKEN));

        final String dataStoreName = request.getPathVariable(VAR_DATA_STORE);
        final Json parameters = request.getParameters();

        final DataStoreResponse response = internalDataStoreFindDocuments(dataStoreName, parameters);

        logger.info(String.format("Data store - count [%s]", response.getItems().size()));
        return Json.map()
                .set(Parameter.DATA_STORE_TOTAL, response.getItems().size());
    }

    @EndpointWebService(path = URL_DATA_STORE_BY_ID, methods = RestMethod.GET) 
    public Json endpointServicesEndpointDataStoreFindDocumentById(WebServiceRequest request){
        logger.info("Data store - find by id");
        checkToken(request.getHeader(Parameter.TOKEN));

        final String dataStoreName = request.getPathVariable(VAR_DATA_STORE);
        final String documentId = request.getPathVariable(VAR_DOCUMENT_ID);

        return internalDataStoreFindDocumentById(dataStoreName, documentId, true);
    }

    @EndpointWebService(path = URL_DATA_STORE, methods = RestMethod.GET) 
    public Json endpointServicesEndpointDataStoreFindDocuments(WebServiceRequest request){
        logger.info("Data store - find");
        checkToken(request.getHeader(Parameter.TOKEN));

        final String dataStoreName = request.getPathVariable(VAR_DATA_STORE);
        final Json parameters = request.getParameters();

        final DataStoreResponse response = internalDataStoreFindDocuments(dataStoreName, parameters);

        logger.info(String.format("Data store - found [%s]", response.getItems().size()));
        return Json.map()
                .set(Parameter.DATA_STORE_ITEMS, response.getItems())
                .set(Parameter.DATA_STORE_TOTAL, response.getTotal())
                .set(Parameter.PAGINATION_OFFSET, response.getOffset());
    }

    @EndpointWebService(path = URL_DATA_STORE_BY_ID, methods = RestMethod.DELETE) 
    public Json endpointServicesEndpointDataStoreRemoveDocumentById(WebServiceRequest request){
        logger.info("Data store - remove");
        checkToken(request.getHeader(Parameter.TOKEN));

        boolean removed = false;

        final String documentId = request.getPathVariable(VAR_DOCUMENT_ID);
        if(StringUtils.isNotBlank(documentId)) {
            final String dataStoreName = request.getPathVariable(VAR_DATA_STORE);
            final Json oldDocument = internalDataStoreFindDocumentById(dataStoreName, documentId, false);
            if(oldDocument != null && !oldDocument.isEmpty()){
                final String oldId = oldDocument.string(DATA_STORE_ID);
                if(StringUtils.isNotBlank(oldId)){
                    removed = dataStore.removeById(oldId);
                }
            }
        }

        logger.info(String.format("Data store - removed [%s]", removed));
        return Json.map()
                .set(Parameter.DATA_STORE_RESULT, removed)
                .set(Parameter.DATA_STORE_TOTAL, removed ? 1 : 0);
    }

    @EndpointWebService(path = URL_DATA_STORE, methods = RestMethod.DELETE) 
    public Json endpointServicesEndpointDataStoreRemoveDocuments(WebServiceRequest request){
        logger.info("Data store - remove all");
        checkToken(request.getHeader(Parameter.TOKEN));

        final String dataStoreName = request.getPathVariable(VAR_DATA_STORE);
        boolean removed = false;
        if(StringUtils.isNotBlank(dataStoreName)) {
            final Json parameters = request.getParameters();
            final Json filter = internalDataStoreFilter(dataStoreName, parameters);

            removed = dataStore.remove(filter);
        }

        logger.info(String.format("Data store - removed all [%s]", removed));
        return Json.map()
                .set(Parameter.DATA_STORE_RESULT, removed)
                .set(Parameter.DATA_STORE_TOTAL, removed ? 1 : 0);
    }

    @EndpointWebService(path = URL_LOCK, methods = RestMethod.POST) 
    public Json endpointServicesEndpointLockEndpointKey(WebServiceRequest request){
        logger.info("Lock key request received");
        checkToken(request.getHeader(Parameter.TOKEN));

        final String key = request.getPathVariable(VAR_KEY);
        boolean acquired = locks().lock(key);
        logger.info(String.format("Lock acquired [%s]: %s", key, acquired));

        return Json.map().set(Parameter.LOCK_ACQUIRED, acquired);
    }

    @EndpointWebService(path = URL_LOCK, methods = RestMethod.DELETE) 
    public Json endpointServicesEndpointUnlockEndpointKey(WebServiceRequest request){
        logger.info("Unlock key request received");
        checkToken(request.getHeader(Parameter.TOKEN));

        final String key = request.getPathVariable(VAR_KEY);
        boolean lockReleased = locks().unlock(key);
        logger.info(String.format("Lock released [%s]: %s", key, lockReleased));

        return Json.map().set(Parameter.LOCK_RELEASED, lockReleased);
    }

    @EndpointWebService(path = URL_CLEAR_CACHE, methods = RestMethod.PUT)
    public Json endpointServicesClearCache(WebServiceRequest request){
        logger.info("Clear cache");
        checkToken(request.getHeader(Parameter.TOKEN));

        management().clearCache();

        return Json.map();
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Helpers
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /** Check the token of the request */
    private void checkToken(String token){
        if(StringUtils.isNotBlank(endpointToken) && !endpointToken.equals(token)){
            throw EndpointException.permanent(ErrorCode.API, "Invalid endpoint services token (check proxy configuration page)");
        }
    }

    private Json internalDataStoreSaveDocument(String label, String dataStoreName, String documentId, Json document){
        document.set(DATA_STORE_NAME, dataStoreName);

        String id = documentId;
        if(StringUtils.isBlank(id)) {
            id = document.string(DATA_STORE_ID);
            if (StringUtils.isBlank(id)) {
                id = Strings.randomUUIDString();
            }
        }
        document.set(DATA_STORE_NEW_ID, id)
                .remove(DATA_STORE_ID);

        final Json oldDocument = internalDataStoreFindDocumentById(dataStoreName, id, false);
        if(oldDocument != null && !oldDocument.isEmpty()){
            final String oldId = oldDocument.string(DATA_STORE_ID);
            if(StringUtils.isNotBlank(oldId)){
                document.set(DATA_STORE_ID, oldId);
            }
        }

        final Json response = dataStore.save(document);
        final String internalId = response.string(DATA_STORE_NEW_ID);
        logger.info(String.format("Data store - %s [%s], internal [%s]", label, id, internalId));

        document.set(DATA_STORE_ID, internalId)
                .remove(DATA_STORE_NAME)
                .remove(DATA_STORE_NEW_ID);
        return document;
    }

    private Json internalDataStoreFilter(String dataStoreName, Json parameters){
        final Json filter = Json.map();
        filter.set(DATA_STORE_NAME, dataStoreName);

        if(parameters != null && !parameters.isEmpty()){
            parameters.forEachMapString((key, value) -> {
                if(DATA_STORE_ID.equals(key)){
                    filter.set(DATA_STORE_NEW_ID, value);
                } else {
                    filter.set(key, value);
                }
            });
        }
        return filter;
    }

    private DataStoreResponse internalDataStoreFindDocuments(String dataStoreName, Json parameters){
        final Json filter = internalDataStoreFilter(dataStoreName, parameters);

        final DataStoreResponse response = dataStore.find(filter);
        final List<Json> items = response.getItems();
        items.forEach(document -> document
                .set(DATA_STORE_ID, document.string(DATA_STORE_NEW_ID))
                .remove(DATA_STORE_NEW_ID)
                .remove(DATA_STORE_NAME)
        );

        return new DataStoreResponse(items, response.getTotal(), response.getOffset());
    }

    private Json internalDataStoreFindDocumentById(String dataStoreName, String documentId, boolean convert){
        if(StringUtils.isBlank(dataStoreName) || StringUtils.isBlank(documentId)){
            return Json.map();
        }
        final Json filter = Json.map()
                .set(DATA_STORE_NAME, dataStoreName)
                .set(DATA_STORE_NEW_ID, documentId);

        final DataStoreResponse response = dataStore.find(filter);
        final List<Json> items = response.getItems();

        Json result = items.isEmpty() ? null : items.get(0);
        if(result == null){
            result = Json.map();
            logger.info(String.format("Data store - not found [%s]", documentId));
        } else {
            if(convert) {
                result = result.set(DATA_STORE_ID, result.string(DATA_STORE_NEW_ID))
                        .remove(DATA_STORE_NAME)
                        .remove(DATA_STORE_NEW_ID);
            }
            logger.info(String.format("Data store - found [%s]", documentId));
        }
        return result;
    }
}
