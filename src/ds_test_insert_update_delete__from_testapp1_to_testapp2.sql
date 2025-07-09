--for insert command in testapp1
INSERT INTO public.ds_change_outbox(
	object_database, object_table, object_pk_attribute, object_attribute, object_id, object_value, object_operation, object_operation_command, updated_by_server_id, update_datetime, updated_by_user_id, object_id_old, object_value_old, note1)
	VALUES ('testapp1', 'sales_order', 'order_id', 'description', 'ord2', 'old order', 'I', 'INSERT INTO public.sales_order(order_id, order_type, description, product_type_name, customer_id, quantity, order_date, update_datetime, updated_by_user, updated_by_server) VALUES (~ord2~,~wholesale~,~old order~,~desktop~,~c1~,~100~,~5/25/2025~,~06/25/2025  10:30:00 AM~,~mtp~,~server1~);', 'server1', '06/25/2025  10:30:00 AM', 'mtp', 'tbd', 'tbd', 'tbd');

--run ds_change_transport.py to copy insert script from testapp1 ds_change_outbox to testapp2 ds_change_inbox
--run ds_change_import.py to run insert command in testapp2
--check if new row is created in testapp2
--SELECT * FROM public.sales_order 


--for update command in test app2
INSERT INTO public.ds_change_outbox(
	object_database, object_table, object_pk_attribute, object_attribute, object_id, object_value, object_operation, object_operation_command, updated_by_server_id, update_datetime, updated_by_user_id, object_id_old, object_value_old, note1)
	VALUES ('testapp1', 'sales_order', 'order_id', 'description', 'ord2', 'old order', 'U', 'UPDATE sales_order set description=~old order change 2~ where order_id=~ord2~', 'server1', '06/25/2025  10:30:00 AM', 'mtp', 'tbd', 'tbd', 'tbd');

--run ds_change_transport.py to copy insert script from testapp1 ds_change_outbox to testapp2 ds_change_inbox
--run ds_change_import.py to run update command in testapp2
--check if new row is created in testapp2update in sales_order 

--for delete command
INSERT INTO public.ds_change_outbox(
	object_database, object_table, object_pk_attribute, object_attribute, object_id, object_value, object_operation, object_operation_command, updated_by_server_id, update_datetime, updated_by_user_id, object_id_old, object_value_old, note1)
	VALUES ('testapp1', 'sales_order', 'order_id', 'description', 'ord2', 'old order', 'D', 'delete from sales_order where order_id=~ord2~', 'server1', '06/25/2025  10:30:00 AM', 'mtp', 'tbd', 'tbd', 'tbd');

--run ds_change_transport.py to copy delete script from testapp1 ds_change_outbox to testapp2 ds_change_inbox
--run ds_change_import.py to run delete command in testapp2
--check if new row is created in testapp2update in sales_order 