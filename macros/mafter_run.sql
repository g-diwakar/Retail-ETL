{% macro mafter_run(results) %}

    {% for run_result in results %}

        {% set run_result_dict = run_result.to_dict() %}

        {% set c_name = run_result_dict.get('node').get('name').split("_")[-2] %}

        {% set c_status = run_result_dict.get('status').upper() %}

        {% set c_loc = run_result_dict.get('node').get('name').split("_")[0] %}

        {% set mmap_bookmark = {'V':'AFTER_STAGE','TMP':'AFTER_TMP','DWH':'COMPLETE'} %}

        {% set c_bookmark = mmap_bookmark.get(c_loc) %}

        {% set sql_query %}

            UPDATE DW_LOGS.LOG_TABLE as t1
            SET STATUS = '{{c_status}}',
                BOOKMARK = '{{c_bookmark}}'
            FROM DW_LOGS.LOG_TABLE as t2
            WHERE t2.PACKAGE_NAME = '{{c_name}}' AND t2.RUN_DATE = CURRENT_DATE AND t1.PACKAGE_NAME = t2.PACKAGE_NAME AND t1.RUN_DATE = t2.RUN_DATE

        {% endset %}

        {% do run_query(sql_query) %}

    {% endfor %}

{% endmacro %}