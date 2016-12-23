import mysql.connector
f = open('./loc-gowalla_totalCheckins.txt', 'rb')

cnx = mysql.connector.connect(user='final', host= 'check-in.cm2toc8pkwqx.us-east-1.rds.amazonaws.com', 
        password= 'final123', database='final')
cursor = cnx.cursor()
data = f.read()
add_column = ("INSERT INTO checkin "
        "(Person, Time, Latitude, Longitude, Position) "
        "VALUES (%s, %s, %s, %s, %s)")
count = 0
dat = []
for line in data.split('\n'):
	count += 1
	if (count <= 6440000):
		continue
	if (len(line.split('\t')) != 5):
		continue
	person = line.split('\t')[0]
	timestamp = line.split('\t')[1][:-1]
	latitude = line.split('\t')[2]
	longitude = line.split('\t')[3]
	location = line.split('\t')[4]
	tup = (person, timestamp, latitude, longitude, location)
	timestamp = timestamp.split('T')[0] + ' ' + timestamp.split('T')[1]
	dat.append(tup)
	#print(count)
	if (count % 10000 == 0):
		try:
			cursor.executemany(add_column, dat)
			dat = []
		except Exception as e:
			print e
		print(count)
		cnx.commit()
print(count)
print(dat)
cursor.executemany(add_column, dat)
cnx.commit()

