{% macro uuid_v5(name_expr, namespace_uuid) %}
{#-
    BigQuery-safe UUID v5 (SHA-1) builder.
    Usage (in a model):
        {{ uuid_v5("exportDataId", "3bfbbd36-410b-43fa-999a-bb990a6db5c7") }} AS correlation_id

    Params:
        name_expr:     SQL expression that evaluates to STRING (e.g. "exportDataId")
        namespace_uuid: a UUID string like "3bfbbd36-410b-43fa-999a-bb990a6db5c7"
-#}
    (
        SELECT UPPER(CONCAT(
            SUBSTR(hx_vr, 1, 8), '-',
            SUBSTR(hx_vr, 9, 4), '-',
            SUBSTR(hx_vr, 13, 4), '-',
            SUBSTR(hx_vr, 17, 4), '-',
            SUBSTR(hx_vr, 21)
        ))
        FROM (
            SELECT CONCAT(
            SUBSTR(hx_v, 1, 16),
            CASE LOWER(SUBSTR(hx_v, 17, 1))
                WHEN '0' THEN '8' WHEN '4' THEN '8' WHEN '8' THEN '8' WHEN 'c' THEN '8'
                WHEN '1' THEN '9' WHEN '5' THEN '9' WHEN '9' THEN '9' WHEN 'd' THEN '9'
                WHEN '2' THEN 'a' WHEN '6' THEN 'a' WHEN 'a' THEN 'a' WHEN 'e' THEN 'a'
                WHEN '3' THEN 'b' WHEN '7' THEN 'b' WHEN 'b' THEN 'b' WHEN 'f' THEN 'b'
            END,
            SUBSTR(hx_v, 18)
            ) AS hx_vr
            FROM (
                SELECT CONCAT(SUBSTR(hx, 1, 12), '5', SUBSTR(hx, 14)) AS hx_v
                FROM (
                    SELECT SUBSTR(
                            TO_HEX(SHA1(CONCAT(
                            FROM_HEX('{{ namespace_uuid | replace("-", "") | lower }}'),
                            CAST('{{ name_expr }}' AS BYTES)
                            ))),
                            1, 32
                        ) AS hx
                )
            )
        )
    )
{% endmacro %}
