#moelmap_postgres_etl.py
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

#Block 0 read ds_program_parameter table for specific row
# --- Database 0 Connection and Data Fetch from ds_program_parameter
db_name1a = 'testapp1'
db_connectionstring1a = 'postgresql://postgres:Welcome123@localhost:5432/'
engine1a = create_engine(db_connectionstring1a + db_name1a)
Session1a = sessionmaker(bind=engine1a)
session1a = Session1a()

program_parameter_id='1001'
queryselect1a = text("""
SELECT id,programtype,description,active_flag,status,fromserver,fromdb,fromtable,fromconnectionstring,fromoutboxpointer,toserver,todb,totable,toconnectionstring,sort_ty,sort_seq,org_id,update_datetime,updated_by_user,updated_by_server
from ds_program_parameter where id=:program_parameter_id
    """)


try:
    result = session1a.execute(queryselect1a, {
                    'program_parameter_id': program_parameter_id
                    })
    results1a = result.fetchall()

    for row in results1a:
     print(row)
     id = row.id
     programtype = row.programtype
     description = row.description
     fromserver = row.fromserver
     fromdb = row.fromdb
     fromtable = row.fromtable
     fromconnectionstring = row.fromconnectionstring
     fromoutboxpointer = row.fromoutboxpointer
     toserver = row.toserver
     todb = row.todb
     totable = row.totable
     toconnectionstring = row.toconnectionstring
except Exception as e:
    print(f"An error occurred during getting ds_program_parameter data: {e}")
finally:
    #session1a.close()
    print("ds_program_parameter is read")

#use fromdb and fromconnection to get the max_id of ds_change. replace the 
# --- Database 1 read from outbox ---
db_name1 = fromdb
db_connectionstring1 = fromconnectionstring
engine1 = create_engine(db_connectionstring1 + db_name1)
Session1 = sessionmaker(bind=engine1)
session1 = Session1()

queryselect1b = text("""
select max(id) as inbox_max_id from ds_change_inbox;
    """)

try:
    result = session1.execute(queryselect1b, {
                    'id': id
                    })
    results1a = result.fetchall()
    for row in results1a:
     inbox_max_id = row.inbox_max_id

except Exception as e:
    print(f"An error occurred during gettin max_id: {e}")
finally:
    #session1.close()
    print("max_id is read.")


# --- Database 2 insert into inbox ---
db_name2 = todb
db_connectionstring2 = toconnectionstring
engine2 = create_engine(db_connectionstring2 + db_name2)
Session_insert = sessionmaker(bind=engine2)
session_insert = Session_insert()

queryselect_inbox = text("""
        SELECT id, object_database, object_table, object_pk_attribute, object_attribute, object_id, object_value, object_operation, object_operation_command, updated_by_server_id, update_datetime, updated_by_user_id, object_id_old, object_value_old, note1, source_id	FROM ds_change_inbox 
        WHERE  id>:fromoutboxpointer and id<=:inbox_max_id
        """)

queryinsert_inbox = text("""
        INSERT INTO ds_change_inbox  (object_database, object_table, object_pk_attribute, object_attribute, object_id, object_value, object_operation, object_operation_command, updated_by_server_id, update_datetime, updated_by_user_id, object_id_old, object_value_old, note1, source_id)
        VALUES (:object_database, :object_table, :object_pk_attribute, :object_attribute, :object_id, :object_value, :object_operation, :object_operation_command, :updated_by_server_id, :update_datetime, :updated_by_user_id, :object_id_old, :object_value_old, :note1, :source_id)
        """)

queryselect_update_datetime = text("""
        SELECT object_id FROM view_recordlevel_update_datetime  
        WHERE  update_datetime < :update_datetime and object_table='sales_order' and object_id='ord2'
        """)

try:
    result = session1.execute(queryselect_inbox, {
                    'fromoutboxpointer': fromoutboxpointer,
                    'inbox_max_id': inbox_max_id
                    })
    results1 = result.fetchall()

    for row in results1:
        print(row) # This will print the entire Row object
        #put the following in array / list rather than in hard coded variables
        id= row.id
        object_database= row.object_database
        object_table= row.object_table
        object_pk_attribute= row.object_pk_attribute
        object_attribute= row.object_attribute
        object_id= row.object_id
        object_value= row.object_value
        object_operation= row.object_operation
        object_operation_command= row.object_operation_command
        updated_by_server_id= row.updated_by_server_id
        update_datetime= row.update_datetime
        updated_by_user_id= row.updated_by_user_id
        object_id_old= row.object_id_old
        object_value_old= row.object_value_old
        note1= row.note1
        source_id= row.source_id

        if programtype=='inbox to apptable import':
            if object_operation=='U':
                queryselect_update_datetime = text("""
                SELECT object_id FROM view_recordlevel_update_datetime  
                WHERE  cast(update_datetime as timestamp) > cast(:update_datetime as timestamp) and object_table=:object_table and object_id=:object_id
                """)
                result_update_time = session_insert.execute(queryselect_update_datetime, {
                        'update_datetime': update_datetime,
                        'object_table': object_table,
                        'object_id': object_id
                        })
                results1_update_time = result_update_time.fetchone()
                print(results1_update_time)

            if object_operation=='U' and results1_update_time==None:  #means nothing higher than incoming, so overwrite is ok
                object_operation_command=object_operation_command.replace("~","'")
                print(object_operation_command)
                queryupdate = text(object_operation_command)
                result_update = session_insert.execute(queryupdate)
                session_insert.commit()

            if object_operation!='U':
                object_operation_command=object_operation_command.replace("~","'")
                print(object_operation_command)
                queryupdate = text(object_operation_command)
                result_update = session_insert.execute(queryupdate)
                session_insert.commit()
    last_success_id=id
except Exception as e:
    # It's good practice to rollback the session_insert if an error occurs during the loop
    # before closing, to ensure no partial inserts are committed.
    session_insert.rollback()
    print(f"An error while read from outbox in write to inbox: {e}")
finally:
    session1.close()
    session_insert.close()
    print("session1 and seeion_insert closed.")

queryupdate1b = text("""
        UPDATE ds_program_parameter set fromoutboxpointer=:inbox_max_id  
        WHERE  id=:program_parameter_id;
        """)

try:
    result = session1a.execute(queryupdate1b, {
                    'inbox_max_id': last_success_id,
                    'program_parameter_id': program_parameter_id
                    })
    #results1a = result.fetchall()
    session1a.commit()
    #print(results1a)
except Exception as e:
    session1a.rollback()
    print(f"An error occurred during updating max_id ds_program_parameter data: {e}")
finally:
    #session1a.close()
    print("ds_program_parameter max counter updated")

