{% macro is_zimbabwe_city(city_col) %}
    LOWER({{ city_col }}) IN (
        'harare', 'bulawayo', 'mutare', 'gweru', 'kwekwe', 'kadoma',
        'masvingo', 'chinhoyi', 'norton', 'marondera', 'ruwa', 'chitungwiza',
        'bindura', 'zvishavane', 'chegutu', 'victoria falls', 'kariba',
        'hwange', 'beitbridge', 'plumtree'
    )
{% endmacro %}


{% macro price_band(price_col, listing_type_col) %}
    CASE
        WHEN {{ listing_type_col }} = 'rent' THEN
            CASE
                WHEN {{ price_col }} < 300    THEN 'low_rent'
                WHEN {{ price_col }} < 700    THEN 'mid_rent'
                WHEN {{ price_col }} < 1500   THEN 'upper_mid_rent'
                ELSE 'luxury_rent'
            END
        ELSE  -- sale
            CASE
                WHEN {{ price_col }} < 30000   THEN 'entry_level'
                WHEN {{ price_col }} < 100000  THEN 'affordable'
                WHEN {{ price_col }} < 300000  THEN 'mid_market'
                WHEN {{ price_col }} < 600000  THEN 'upper_market'
                ELSE 'luxury'
            END
    END
{% endmacro %}


{% macro months_ago(n) %}
    DATE_TRUNC('MONTH', DATEADD('MONTH', -{{ n }}, CURRENT_DATE()))
{% endmacro %}
