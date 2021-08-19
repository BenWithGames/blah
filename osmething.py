import pymysql.cursors
from datetime import datetime
import json

unsub = """
[
    {"contactstring":"123456789", "timestamp":"2022-06-09 12:00:00", "action" : "resume"},
    {"contactstring":"543234565", "timestamp":"2022-06-09 12:01:00", "action" : "stop"}
]
"""

resume = """
[
    {"contactstring":"123456789", "timestamp":"2021-06-09 10:00:00", "program":"program_name", "template":"template_name"},
    {"contactstring":"5432341254", "timestamp":"2021-06-09 3:00:00", "program":"program_name", "template":"template_name"},
    {"contactstring":"5432341254", "timestamp":"2021-06-09 5:00:00", "program":"program_name1", "template":"template_name"},
    {"contactstring":"7653457624", "timestamp":"2021-06-09 13:00:00", "program":"program_name", "template":"template_name"},
    {"contactstring":"6663451345", "timestamp":"2021-06-09 13:00:00", "program":"program_name", "template":"template_name"}
]
"""
selectContact = "SELECT * FROM contact WHERE mobile_number = %s"
selectProgram = "SELECT * FROM program WHERE mobile_number = %s AND keyword = %s"

updateUnsub = "UPDATE program SET unsubscribe_timestamp = %s, active = %s WHERE mobile_number = %s"
updateResume = "UPDATE program SET continue_timestamp = %s, active = %s WHERE mobile_number = %s"

insertContactIfMissing = "INSERT INTO contact (mobile_number) VALUES (%s)"
insertSignupProgramIfMissing = "INSERT INTO program (mobile_number, keyword, approved_timestamp, active) VALUES (%s, %s, %s, %s)"
insertUnsubProgramIfMissing = "INSERT INTO program (mobile_number, unsubscribe_timestamp, active) VALUES (%s, %s, %s)"
insertResumeProgramIfMissing = "INSERT INTO program (mobile_number, continue_timestamp, active) VALUES (%s, %s, %s)"


def insertSql(events):
    connection = getConnection()
    with connection:
        with connection.cursor() as cursor:
            for obj in events:
                if obj['file'] == "resume":
                    processSignup(obj, cursor, connection)
                if obj['file'] == "unsub":
                    processUnsub(obj, cursor, connection)

def processSignup(obj, cursor, connection):
    cursor.execute(selectContact, obj['contactstring'])
    rows = cursor.fetchone()
    if (rows == None):
        cursor.execute(insertContactIfMissing, obj['contactstring'])
        connection.commit()
    cursor.execute(selectProgram, (obj['contactstring'], obj['program']))
    rows2 = cursor.fetchone()
    if (rows2 == None):
         cursor.execute(insertSignupProgramIfMissing, (obj['contactstring'], obj['program'], obj['timestamp'], True))


def processUnsub(obj, cursor, connection):
    cursor.execute(selectContact, obj['contactstring'])
    rows = cursor.fetchone()
    print(obj)
    # Insert new records if user doesn't exist
    if (rows == None):
        # Insert into contact
        cursor.execute(insertContactIfMissing, obj['contactstring'])
        # Insert into program
        if (obj['action'] == 'stop'):
            cursor.execute(insertUnsubProgramIfMissing, (obj['contactstring'], obj['timestamp'], False))
            connection.commit()
        else:
            cursor.execute(insertResumeProgramIfMissing, (obj['contactstring'], obj['timestamp'], True))
            connection.commit()
    # Update records
    else:
        if (obj['action'] == 'stop'):
            cursor.execute(updateUnsub, (obj['timestamp'], False, obj['contactstring']))
        else:
            cursor.execute(updateResume, (obj['timestamp'], True, obj['contactstring']))
        connection.commit()

def processEvents():
    resumeJson = json.loads(resume)
    unsubJson = json.loads(unsub)
    for obj in resumeJson:
        obj.update({"file" :"resume"})
    for obj in unsubJson:
        obj.update({"file" : "unsub"})
    events = [resumeJson, unsubJson]
    compiledEvents = compileEvents(events)
    sortedEvents = sortEvents(compiledEvents)
    return sortedEvents

def compileEvents(events):
    compiledEvents = []
    for event in events:
        for obj in event:
            compiledEvents.append(obj)
    return compiledEvents

def sortEvents(events):
    sortedEvents = sorted(
        events,
        key=lambda x: datetime.strptime(x['timestamp'], '%Y-%m-%d %H:%M:%S')
    )

    return sortedEvents

def getConnection():
    connection = pymysql.connect(host='localhost',
                                 user='bmarkham',
                                 password='',
                                 database='test',
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection

if __name__ == '__main__':
    events = processEvents()
    insertSql(events)