{% macro mpre_hook(c_package,c_status, c_bookmark) %}

    {% set sql_query %}
        SELECT PACKAGE_NAME from DW_LOGS.LOG_TABLE WHERE PACKAGE_NAME = '{{c_package}}' and RUN_DATE = CURRENT_DATE
    {% endset %}

    {% set result = run_query(sql_query) %}

    {% if result | length <= 0 %}

        {% set sql_query %}
            INSERT INTO DW_LOGS.LOG_TABLE VALUES ('{{c_package}}',CURRENT_DATE,1,'{{c_status}}','{{c_bookmark}}')
        {% endset %}

    {% else %}

        {%set sql_query %}
            UPDATE DW_LOGS.LOG_TABLE as t1
            SET STATUS = '{{c_status}}',
                BOOKMARK = '{{c_bookmark}}',
                RUN_ID = t1.RUN_ID + 1
            FROM DW_LOGS.LOG_TABLE as t2
            WHERE t2.PACKAGE_NAME = '{{c_package}}' AND t2.RUN_DATE = CURRENT_DATE AND t1.PACKAGE_NAME = t2.PACKAGE_NAME AND t1.RUN_DATE = t2.RUN_DATE

        {% endset %}

    {% endif %}

    {% do run_query(sql_query) %}


{% endmacro %}


