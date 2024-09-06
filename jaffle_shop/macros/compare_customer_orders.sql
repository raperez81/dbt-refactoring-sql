{% macro compare_customer_orders() %}
    {% set old_relation = adapter.get_relation(
        database = "analytics_dev",
        schema = "test",
        identifier = "customer_orders_legacy"
    ) -%}

    {% set dbt_relation = ref('customer_orders') %}

    {% set result = audit_helper.compare_relations(
        a_relation = old_relation,
        b_relation = dbt_relation,
        primary_key = "order_id"
    ) %}
    {{ return(result) }}
{% endmacro %}