-- Seed AGENT_GLOSSARY with domain vocabulary extracted from
-- api/services/dbt_mcp_service.py lines 337-353 (status_values,
-- business_terms, entity_relationships).
-- Idempotent: INSERT OR IGNORE semantics via MERGE.

MERGE INTO DEMO_BSC.AGENT_GLOSSARY AS tgt
USING (
  SELECT * FROM VALUES
    -- order status enum values (category: order_status)
    ('CREATED',            'order_status',       'Order has been created but not yet allocated',                                   NULL::VARIANT, TRUE),
    ('ALLOCATED',          'order_status',       'Order has been allocated to inventory but not yet picked',                       NULL::VARIANT, TRUE),
    ('PICKED',             'order_status',       'Order items have been picked from inventory',                                    NULL::VARIANT, TRUE),
    ('SHIPPED',            'order_status',       'Order has been shipped to the destination facility',                             NULL::VARIANT, TRUE),
    ('DELIVERED',          'order_status',       'Order has been delivered to the destination facility',                           NULL::VARIANT, TRUE),
    ('BACKORDERED',        'order_status',       'Order cannot be fulfilled due to insufficient inventory',                        NULL::VARIANT, TRUE),
    ('CANCELLED',          'order_status',       'Order has been cancelled and will not be fulfilled',                             NULL::VARIANT, TRUE),
    ('ON_HOLD',            'order_status',       'Order processing is paused pending further action',                              NULL::VARIANT, TRUE),
    -- business terms (category: business_term)
    ('is_fulfilled',       'business_term',      'order has been SHIPPED or DELIVERED',                                           NULL::VARIANT, TRUE),
    ('priority_flag',      'business_term',      'TRUE when the order is flagged as urgent / priority',                           NULL::VARIANT, TRUE),
    ('fulfillment_rate',   'business_term',      'percentage of orders that are shipped or delivered',                            NULL::VARIANT, TRUE),
    ('priority_rate',      'business_term',      'percentage of orders flagged as priority',                                      NULL::VARIANT, TRUE),
    ('days_to_last_update','business_term',      'calendar days from order creation to most recent status change',                 NULL::VARIANT, TRUE),
    -- entity relationships (category: entity_relationship)
    ('order_customer',     'entity_relationship','Each order belongs to exactly one customer (customer_account_id)',               NULL::VARIANT, TRUE),
    ('order_facility',     'entity_relationship','Each order ships to exactly one facility (facility_id)',                         NULL::VARIANT, TRUE),
    ('facility_location',  'entity_relationship','Facilities have city, state, and zip attributes',                               NULL::VARIANT, TRUE)
) AS src (term, category, definition, metadata, active)
ON tgt.term = src.term
WHEN NOT MATCHED THEN
  INSERT (term, category, definition, metadata, active)
  VALUES (src.term, src.category, src.definition, src.metadata, src.active);
