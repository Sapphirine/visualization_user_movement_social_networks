from flask import Flask, render_template, request, redirect, url_for
from random import uniform, randint
import json
from test import get_checkin, get_pairs, recommend_friends, recommend_location, get_social

app = Flask(__name__)

@app.route('/map')
@app.route('/')
def show_map():
	try:
		x = int(request.args.get('x'))
	except Exception:
		x = uniform(-90, 90)
	try:
		y = int(request.args.get('y'))
	except Exception:
		y = uniform(-180, 180)
	return render_template('map.html', x=x, y=y)

@app.route('/pairs')
def show_pairs():
    try:
    	id1 = int(request.args.get('id1'))
    except Exception:
    	id1 = randint(1, 1000)
    try:
    	id2 = int(request.args.get('id2'))
    except Exception:
    	id2 = randint(1, 1000)
    pairArray, counts = get_pairs(id1, id2)
    results = [(result[0][0], result[0][1], result[1]) for result in counts]
    if (len(results) == 0):
    	x = 40.81
    	y= -73.96
    else:
    	x = results[0][0]
    	y = results[0][1]
    print(results)
    return render_template('pair.html', x=x, y=y, list=results)

@app.route('/social')
def show_social():
	try:
		id = int(request.args.get('id'))
	except Exception:
		id = randint(1, 1000)
	try:
		maxnum = int(request.args.get('maxnum'))
	except Exception:
		maxnum = 1000
	try:
		minnum = int(request.args.get('minnum'))
	except Exception:
		minnum = 0
	results = get_social(id, minnum, maxnum)
	return render_template('social.html', pivot=id, list=results)

@app.route('/id')
def show_id():
	try:
		id = int(request.args.get('id'))
	except Exception:
		id = randint(1, 1000)
	results = get_checkin(id)
	x = results[0][0]
	y = results[0][1]
	z = results[0][2]
	results = results[1:]
	print(results)
	return render_template('id.html', x=x, y=y, z=z, list=results)

@app.route('/rec_friend')
def recommend_friend():
	try:
		id = int(request.args.get('id'))
	except Exception:
		id = randint(1, 1000)
	try:
		maxnum = int(request.args.get('maxnum'))
	except Exception:
		maxnum = 10
	results = recommend_friends(id)
	results = results[:maxnum]
	print(results)
	return render_template('rec_friends.html', pivot=id, list=results)

@app.route('/rec_location')
def recommend_location2():
	try:
		id = int(request.args.get('id'))
	except Exception:
		id = randint(1, 1000)
	try:
		maxnum = int(request.args.get('maxnum'))
	except Exception:
		maxnum = 10
	results = recommend_location(id, maxnum)
	results = results[:maxnum]
	print(results)
	encode = {0:"midnight ", 1:"morning ", 2:"afternoon ", 3:"evening "}
	dict_time = {}
	for result in results:
		if ((result[0][0], result[0][2], result[0][3]) in dict_time):
			dict_time[(result[0][0], result[0][2], result[0][3])] += encode[result[0][1]]
		else:
			dict_time[(result[0][0], result[0][2], result[0][3])] = encode[result[0][1]]
	inputs = dict_time.items()
	print(inputs)
	inputs = [(inp[0][1], inp[0][2], inp[0][0], inp[1][:-1]) for inp in inputs]
	print(inputs)
	x = inputs[0][0]
	y = inputs[0][1]
	print(x)
	print(y)
	return render_template('rec_location2.html', x=x, y=y, list=inputs)

@app.route('/read_ID', methods=['POST'])
def read_ID():
	try:
		ID = int(request.form['ID'])
		print(ID)
		return redirect(url_for('show_id', id=ID))
	except Exception:
		return redirect(url_for('show_id'))

@app.route('/read_ID2', methods=['POST'])
def read_ID2():
	try:
		ID = int(request.form['ID'])
		print(ID)
		return redirect(url_for('recommend_friend', id=ID))
	except Exception:
		return redirect(url_for('recommend_friend'))

@app.route('/read_ID3', methods=['POST'])
def read_ID3():
	try:
		ID = int(request.form['ID'])
		print(ID)
		return redirect(url_for('recommend_location2', id=ID))
	except Exception:
		return redirect(url_for('recommend_location2'))

@app.route('/read_ID4', methods=['POST'])
def read_ID4():
	try:
		ID = int(request.form['ID'])
		print(ID)
		return redirect(url_for('show_social', id=ID))
	except Exception:
		return redirect(url_for('show_social'))

@app.route('/read_pairs', methods=['POST'])
def read_pairs():
	try:
		ID1 = int(request.form['ID1'])
		ID2 = int(request.form['ID2'])
		print(ID1)
		print(ID2)
		return redirect(url_for('show_pairs', id1=ID1, id2=ID2))
	except Exception:
		return redirect(url_for('show_pairs'))

if __name__ == '__main__':
	app.run(debug = True)

