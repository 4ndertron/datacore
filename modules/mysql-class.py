import mysql.connector

pitt_db = mysql.connector.connect(
    host='liftenergypitt.sftp.wpengine.com',
    user='liftenergypitt',
    password='QUyTPoGFUq3E5BI1hzwd',
    port=13306
).server_port = 13306

print(pitt_db)
