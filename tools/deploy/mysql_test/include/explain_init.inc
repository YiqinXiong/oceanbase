connect (obsys_explain,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_explain;
--disable_query_log
alter system set_tp tp_no = 918, error_code = 4016, frequency = 1;
--enable_query_log
disconnect obsys_explain;
connection default;
