--ds_reset_testapp1
delete FROM ds_change_outbox;
update ds_program_parameter set fromoutboxpointer=0 where id='1000';
update ds_program_parameter set fromoutboxpointer=0 where id='1001';