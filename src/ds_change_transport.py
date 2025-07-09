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

program_parameter_id='1000'
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
select max(id) as outbox_max_id from ds_change_outbox;
    """)
try:
    result = session1.execute(queryselect1b, {
                    'id': id
                    })
    results1a = result.fetchall()
    for row in results1a:
     outbox_max_id = row.outbox_max_id

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

queryselect_outbox = text("""
        SELECT id, object_database, object_table, object_pk_attribute, object_attribute, object_id, object_value, object_operation, object_operation_command, updated_by_server_id, update_datetime, updated_by_user_id, object_id_old, object_value_old, note1, source_id	FROM ds_change_outbox 
        WHERE  id>:fromoutboxpointer and id<=:outbox_max_id
        """)

queryinsert_inbox = text("""
        INSERT INTO ds_change_inbox  (object_database, object_table, object_pk_attribute, object_attribute, object_id, object_value, object_operation, object_operation_command, updated_by_server_id, update_datetime, updated_by_user_id, object_id_old, object_value_old, note1, source_id)
        VALUES (:object_database, :object_table, :object_pk_attribute, :object_attribute, :object_id, :object_value, :object_operation, :object_operation_command, :updated_by_server_id, :update_datetime, :updated_by_user_id, :object_id_old, :object_value_old, :note1, :source_id)
        """)

try:
    result = session1.execute(queryselect_outbox, {
                    'fromoutboxpointer': fromoutboxpointer,
                    'outbox_max_id': outbox_max_id
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

        if programtype=='outbox to inbox transport':
            result_insert = session_insert.execute(queryinsert_inbox, {
                'object_database':object_database,
                'object_table':object_table,
                'object_pk_attribute':object_pk_attribute,
                'object_attribute':object_attribute,
                'object_id':object_id,
                'object_value':object_value,
                'object_operation':object_operation,
                'object_operation_command':object_operation_command,
                'updated_by_server_id':updated_by_server_id,
                'update_datetime':update_datetime,
                'updated_by_user_id':updated_by_user_id,
                'object_id_old':object_id_old,
                'object_value_old':object_value_old,
                'note1':note1,
                'source_id':id
            })
            session_insert.commit()
except Exception as e:
    # It's good practice to rollback the session_insert if an error occurs during the loop
    # before closing, to ensure no partial inserts are committed.
    session_insert.rollback()
    print(f"An error while read from outbox in write to inbox: {e}")
finally:
    session1.close()
    session_insert.close()
    print("session1 and insert_session closed.")

queryupdate1b = text("""
        UPDATE ds_program_parameter set fromoutboxpointer=:outbox_max_id  
        WHERE  id=:program_parameter_id;
        """)

try:
    result = session1a.execute(queryupdate1b, {
                    'outbox_max_id': outbox_max_id,
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