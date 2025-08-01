{% macro bytes4_to_int32(byte_value) %}
    {#- Convert a 4-byte BYTES field to its signed int32 value, returned as INT64 -#}

    {#- 1. Treat the bytes as an unsigned 32-bit integer -#}
    {%- set u32 = "SAFE_CAST(CONCAT('0x', TO_HEX(" ~ byte_value ~ ")) AS INT64)" -%}

    {# 2. Subtract 2^32 to get the two-complement value if the sign bit is set (u32 > 0x7FFFFFFF) #}
    CASE
        WHEN {{ u32 }} > CAST("0x7FFFFFFF" AS INT64)
            THEN {{ u32 }} - CAST("0x100000000" AS INT64)
        ELSE {{ u32 }}
    END

{% endmacro %}


{% macro base64_map(b64_str, index, zero_based=False) %}
    {#- Map a single Base-64 character to its six-bit ordinal (0-63) -#}

    {% set alphabet = "'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'" %}

    {%- if zero_based -%}
        {% set pos1 = "(" ~ index ~ " + 1)" %}
    {%- else -%}
        {% set pos1 = index %}
    {%- endif -%}

    {% set ch = "SUBSTR(" ~ b64_str ~ ", " ~ pos1 ~ ", 1)" %}

    NULLIF(
        CASE
            WHEN {{ ch }} = '' THEN -1
            ELSE STRPOS({{ alphabet }}, {{ ch }}) - 1
        END,
        -1
    )
{%- endmacro %}


{% macro hex_map(hex_str, index, zero_based=False) %}
    {#- Map a single hex character to its 4-bit ordinal (0-15) -#}
    {% set alphabet = "'0123456789abcdef'" %}

    {%- if zero_based -%}
        {% set pos1 = "(" ~ index ~ " + 1)" %}
    {%- else -%}
        {% set pos1 = index %}
    {%- endif -%}

    {%- set ch = "LOWER(SUBSTR(" ~ hex_str ~ ", " ~ pos1 ~ ", 1))" -%}

    NULLIF(
        CASE
            WHEN {{ ch }} = '' THEN -1
            ELSE STRPOS({{ alphabet }}, {{ ch }}) - 1
        END,
        -1
    )
{%- endmacro %}


{% macro reverse_hex_bytes(hex_expr, add_0x=False) %}
    {#- Return a new hex string whose bytes are reversed -#}
    {% set prefix = "'0x'" if add_0x else "''" %}
    (
        SELECT 
            CONCAT(
                {{ prefix }},
                STRING_AGG(byte, '' ORDER BY idx DESC)
            )
        FROM UNNEST(
            REGEXP_EXTRACT_ALL(
                REGEXP_REPLACE({{ hex_expr }}, r'^0x', ''),
                r'..'
            )
        ) AS byte WITH OFFSET idx
    )
{% endmacro %}


{% macro hex_to_int(hex_val) %}
    {#- Convert a hex string (optionally prefixed with 0x/0X) to INT64 -#}
    SAFE_CAST(
        CONCAT(
            '0x',
            REGEXP_REPLACE({{ hex_val }}, r'(?i)^0x', '')
        )
        AS INT64
    )
{% endmacro %}


{% macro digit_to_bitstring(digit, base) %}
    {#- Convert ONE radix-`base` digit (0 ≤ digit < base) to its binary representation -#}
    {%- set width = "CAST(CEIL(LOG("~ base ~") / LOG(2)) AS INT64)" -%}
    (
        SELECT
            STRING_AGG(
                CAST({{ digit }} >> bit_idx & 1 AS STRING),
                ""
                ORDER BY bit_idx DESC
            )
        FROM UNNEST(GENERATE_ARRAY(0, {{ width }} - 1)) AS bit_idx
    )
{% endmacro %}


{% macro value_to_bitstring(value, width) %}
    {#- Convert an integer to a fixed-width binary string -#}
    (
        SELECT
            STRING_AGG(
                CAST({{ value }} >> bit_idx & 1 AS STRING),
                ""
                ORDER BY bit_idx DESC
            )
        FROM UNNEST(GENERATE_ARRAY(0, {{ width }} - 1)) AS bit_idx
    )
{% endmacro %}


{% macro bitstring_to_int(bitstr) %}
    {#- Convert a binary string to its integer representation -#}
    (
    SELECT
        SUM(SAFE_CAST(char AS INT64) << (LENGTH({{ bitstr }}) - 1 - bit_idx))
    FROM
        UNNEST(SPLIT({{ bitstr }}, "")) AS char WITH OFFSET bit_idx
    )
{% endmacro %}


{% macro double_unbiased_exponent(exp_bits) %}
    {#- Decode the 11-bit exponent field of an IEEE-754 double-precision floating-point number and return the unbiased exponent as INT64. -#}
    {%- set exp_bias  = 1023 -%}
    (
        SELECT
            SUM(SAFE_CAST(char AS INT64) << (LENGTH({{ exp_bits }}) - 1 - bit_idx))
        FROM UNNEST(SPLIT({{ exp_bits }}, "")) AS char WITH OFFSET bit_idx
    ) - {{ exp_bias }}
{% endmacro %}


{% macro double_mantissa(frac_bits) %}
    {#- Convert the fractional bit-field of an IEEE-754 double-precision number to its normalised mantissa (aka significand). -#}
    (
        SELECT 1 +
            SUM(
                COALESCE(
                    SAFE_CAST( SUBSTR({{ frac_bits }}, bit_idx + 1, 1 ) AS INT64 ),
                    0
                ) * POW(2, -(bit_idx + 1))
            )
        FROM UNNEST(GENERATE_ARRAY(0, 51)) AS bit_idx
    )
{% endmacro %}


{% macro double_from_components(sign, mantissa, unbiased_exponent) %}
    {#- Assemble an IEEE-754 **double-precision** floating value from its decoded components. -#}
    {#- The formula is: (-1)^sign * mantissa * 2^unbiased_exponent -#}
    ROUND(
        POW(-1, SAFE_CAST({{ sign }} AS INT64)) * {{ mantissa }} * POW(2, {{ unbiased_exponent }}),
        5
    )
{% endmacro %}
