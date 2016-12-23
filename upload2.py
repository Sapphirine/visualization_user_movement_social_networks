import mysql.connector
f = open('./loc-gowalla_edges.txt', 'rb')

cnx = mysql.connector.connect(user='final', host= 'check-in.cm2toc8pkwqx.us-east-1.rds.amazonaws.com', 
        password= 'final123', database='final')
cursor = cnx.cursor()
data = f.read()
add_column = ("INSERT INTO edge "
        "(Person1, Person2) "
        "VALUES (%s, %s)")
count = 0
dat = []
for line in data.split('\n'):
	count += 1
	if (len(line.split('\t')) != 2):
		continue
	person1 = line.split('\t')[0]
	person2 = line.split('\t')[1]
	tup = (person1, person2)
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
cursor.executemany(add_column, dat)
cnx.commit()