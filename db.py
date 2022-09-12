from mysql import connector

HOST = 'localhost'
USER = 'root'
PASSWORD = ''
DATABASE = 'api'
# db credentials and connection
mydb = connector.connect(
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DATABASE
)
mycursor = mydb.cursor(buffered=True)


def create_table():
    sql = """CREATE TABLE IF NOT EXISTS `id` (id VARCHAR(20) NOT NULL, status VARCHAR(20) NULL DEFAULT NULL, PRIMARY KEY (id))"""
    mycursor.execute(sql)
    mydb.commit()
    sql = """CREATE TABLE IF NOT EXISTS `details` (body__id VARCHAR(20) NOT NULL, code INT(3) NULL DEFAULT NULL, PRIMARY KEY (body__id))"""
    mycursor.execute(sql)
    mydb.commit()


def get_access_token():
    mycursor.execute("SELECT * FROM auth")
    myresult = mycursor.fetchone()
    return myresult


def update_access_token(data):
    sql = "UPDATE auth SET access_token = %s, refresh_token = %s WHERE id = 1"
    values = (data['access_token'], data['refresh_token'])
    mycursor.execute(sql, values)
    mydb.commit()


def insert_items_id(data):
    sql = "INSERT IGNORE INTO id (id) VALUES (%s)"
    values = (data)
    mycursor.executemany(sql, values)
    mydb.commit()
    return mycursor.rowcount


def get_items_id():
    mycursor.execute("SELECT id FROM id")
    myresult = mycursor.fetchall()
    return myresult


def make_columns_table(columns):
    for column in columns:
        sql = """SELECT count(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'details' AND table_schema = '%s' AND column_name = '%s'""" % (DATABASE, column)
        mycursor.execute(sql)
        myresult = mycursor.fetchone()
        if myresult[0] == 0:
            sql = """ALTER TABLE details ADD `%s` TEXT NULL DEFAULT NULL""" % column
            mycursor.execute(sql)
            mydb.commit()


def save_details(data):

    for ki, value in data.items():
        # if value['code'] != 200 then set id table status to 'deleted'

        
        sql = """UPDATE id SET status = %s WHERE id = %s""" 
        mycursor.execute(sql, (value['body__status'],value['body__id']))
        mydb.commit()

        # check if item['body/id'] already exists in the table
        sql = "SELECT * FROM details WHERE body__id = %s"
        mycursor.execute(sql, (value['body__id'],))

        myresult = mycursor.fetchone()
        if myresult is None:
            # if not, create a new row
            columns = ', '.join(
                "`" + str(x).replace('/', '_') + "`" for x in value.keys())
            values = ', '.join("'" + str(x).replace("'", "") +
                               "'" for x in value.values())
            sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % (
                'details', columns, values)

            mycursor.execute(sql)
            mydb.commit()
        else:
            # Delete the row
            sql = "DELETE FROM details WHERE `body__id` = %s"
            mycursor.execute(sql, (value['body__id'],))
            mydb.commit()
            
            # Insert the row again
            columns = ', '.join(
                "`" + str(x).replace('/', '_') + "`" for x in value.keys())
            values = ', '.join("'" + str(x).replace("'", "") +
                               "'" for x in value.values())
            sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % (
                'details', columns, values)

            mycursor.execute(sql)
            mydb.commit()

            
            # # update the row
            # sql = "UPDATE details SET "
            # for key, val in value.items():
            #     if myresult[key] != val:
            #         sql += "`" + str(key).replace('/', '_') + "` = '" + str(val).replace("'", "") + "',"
            # sql = sql[:-1] + " WHERE body__id = %s"
            # values = (value['body__id'],)
            # mycursor.execute(sql, values)
            # mydb.commit()
    print('done')


def id_check(id):
    mycursor.execute("SELECT * FROM id WHERE id = %s", (id,))
    #if exitsts then return True
    myresult = mycursor.fetchone()
    if myresult is None:
        return False
    else:
        return True
