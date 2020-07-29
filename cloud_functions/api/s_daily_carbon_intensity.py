import serverless_sdk
sdk = serverless_sdk.SDK(
    org_id='frasertooth',
    application_name='japan-grid-carbon',
    app_uid='GNdgrZ9GzWLXjr5pkz',
    org_uid='CmVS3bdG29xmhr550J',
    deployment_uid='87505630-a5b2-45dc-9b6e-292f42f3aea1',
    service_name='japan-grid-carbon',
    should_log_meta=True,
    should_compress_logs=True,
    disable_aws_spans=False,
    disable_http_spans=False,
    stage_name='dev',
    plugin_version='3.6.18',
    disable_frameworks_instrumentation=False
)
handler_wrapper_kwargs = {'function_name': 'japan-grid-carbon-dev-daily_carbon_intensity', 'timeout': 6}
try:
    user_handler = serverless_sdk.get_user_handler('.daily_carbon_intensity')
    handler = sdk.handler(user_handler, **handler_wrapper_kwargs)
except Exception as error:
    e = error
    def error_handler(event, context):
        raise e
    handler = sdk.handler(error_handler, **handler_wrapper_kwargs)
