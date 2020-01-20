TIME_GRAIN_ADDON_FUNCTIONS = {
    'hive': {
        None: '{col}',
        'PT1S': "from_unixtime(unix_timestamp({col}), 'yyyy-MM-dd HH:mm:ss')",
        'PT1M': "from_unixtime(unix_timestamp({col}), 'yyyy-MM-dd HH:mm:00')",
        'PT1H': "from_unixtime(unix_timestamp({col}), 'yyyy-MM-dd HH:00:00')",
        'P1D': "from_unixtime(unix_timestamp({col}), 'yyyy-MM-dd 00:00:00')",
        'P1W': "date_format(date_sub(nianxian, CAST(7-from_unixtime(unix_timestamp(nianxian),'u') as int)), 'yyyy-MM-dd 00:00:00')",
        'P1M': "from_unixtime(unix_timestamp({col}), 'yyyy-MM-01 00:00:00')",
        'P0.25Y': "date_format(add_months(trunc(nianxian, 'MM'), -(month(nianxian)-1)%3), 'yyyy-MM-dd 00:00:00')",
        'P1Y': "from_unixtime(unix_timestamp({col}), 'yyyy-01-01 00:00:00')",
        'P1W/1970-01-03T00:00:00Z': "date_format(date_add(nianxian, INT(6-from_unixtime(unix_timestamp(nianxian), 'u'))), 'yyyy-MM-dd 00:00:00')",
        '1969-12-28T00:00:00Z/P1W': "date_format(date_add(nianxian, -INT(from_unixtime(unix_timestamp(nianxian), 'u'))), 'yyyy-MM-dd 00:00:00')"
    }
}
