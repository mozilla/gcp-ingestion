package com.mozilla.telemetry.ingestion.core;

public class Constant {

  private Constant() {
  }

  public static class Attribute {

    private Attribute() {
    }

    public static final String APP_BUILD_ID = "app_build_id";
    public static final String APP_NAME = "app_name";
    public static final String APP_UPDATE_CHANNEL = "app_update_channel";
    public static final String APP_VERSION = "app_version";
    public static final String CLIENT_COMPRESSION = "client_compression";
    public static final String CLIENT_ID = "client_id";
    public static final String CLIENT_IP = "client_ip";
    public static final String CONTEXT_ID = "context_id";
    public static final String DATE = "date";
    public static final String DNT = "dnt";
    public static final String DOCUMENT_ID = "document_id";
    public static final String DOCUMENT_NAMESPACE = "document_namespace";
    public static final String DOCUMENT_TYPE = "document_type";
    public static final String DOCUMENT_VERSION = "document_version";
    public static final String GEO_CITY = "geo_city";
    public static final String GEO_COUNTRY = "geo_country";
    public static final String GEO_SUBDIVISION1 = "geo_subdivision1";
    public static final String GEO_SUBDIVISION2 = "geo_subdivision2";
    public static final String GEO_DMA_CODE = "geo_dma_code";
    public static final String ISP_NAME = "isp_name";
    public static final String ISP_ORGANIZATION = "isp_organization";
    public static final String ISP_DB_VERSION = "isp_db_version";
    public static final String HOST = "host";
    public static final String MESSAGE_ID = "message_id";
    public static final String NORMALIZED_APP_NAME = "normalized_app_name";
    public static final String NORMALIZED_CHANNEL = "normalized_channel";
    public static final String NORMALIZED_COUNTRY_CODE = "normalized_country_code";
    public static final String NORMALIZED_OS = "normalized_os";
    public static final String NORMALIZED_OS_VERSION = "normalized_os_version";
    public static final String OS = "os";
    public static final String OS_VERSION = "os_version";
    public static final String PROXY_TIMESTAMP = "proxy_timestamp";
    public static final String REMOTE_ADDR = "remote_addr";
    public static final String REPORTING_URL = "reporting_url";
    public static final String SAMPLE_ID = "sample_id";
    public static final String SUBMISSION_TIMESTAMP = "submission_timestamp";
    public static final String URI = "uri";
    public static final String USER_AGENT = "user_agent";
    public static final String USER_AGENT_BROWSER = "user_agent_browser";
    public static final String USER_AGENT_OS = "user_agent_os";
    public static final String USER_AGENT_VERSION = "user_agent_version";
    public static final String VERSION = "version";
    public static final String X_DEBUG_ID = "x_debug_id";
    public static final String X_FORWARDED_FOR = "x_forwarded_for";
    public static final String X_FOXSEC_IP_REPUTATION = "x_foxsec_ip_reputation";
    public static final String X_LB_TAGS = "x_lb_tags";
    public static final String X_PINGSENDER_VERSION = "x_pingsender_version";
    public static final String X_PIPELINE_PROXY = "x_pipeline_proxy";
    public static final String X_SOURCE_TAGS = "x_source_tags";
    public static final String X_TELEMETRY_AGENT = "x_telemetry_agent";
  }

  public static class FieldName {

    private FieldName() {
    }

    public static final String ADDITIONAL_PROPERTIES = "additional_properties";
    public static final String ATTRIBUTE_MAP = "attributeMap";
    public static final String KEY = "key";
    public static final String LIST = "list";
    public static final String METADATA = "metadata";
    public static final String PAYLOAD = "payload";
  }

  public static class Namespace {

    private Namespace() {
    }

    public static final String TELEMETRY = "telemetry";
  }
}
