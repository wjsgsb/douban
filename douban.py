import requests
from bs4 import BeautifulSoup
import time
import os
import MySQLdb
from DBUtils.PooledDB import PooledDB

start_film_url = 'http://movie.douban.com/subject/2121871/collections'

film_url = 'http://movie.douban.com/people/59402778/collect'

login_data = {
	'source': 'index_nav',
	'form_email': 'songkunsongkun@gmail.com',
	'form_password': 'songkun',
	'remember': 'on'
}

cookies = {
	'bid': '4nn/45yQe2E',
	'll': '118282',
	'ap': '1',
	'ct': 'y',
	'dbcl2': '1249269:0XBM64BkHOk',
	'ck': 'YFnH',
	'push_noty_num': '82',
	'push_doumail_num': '65',
	# '__utma': '30149280.545517482.1432194661.1432574426.1432616392.11',
	# '__utmc': '30149280',
	# '__utmz': '30149280.1432194661.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
	# '__utmv': '30149280.124',
	'ue': 'songkunsongsongkun@gmail.com',
	'ps': 'y'
}

header = {
	'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36',
	'Host': 'movie.douban.com',
	'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
	'Accept-Encoding': 'gzip,deflate,sdch',
	'Accept-Language': 'zh-CN,zh;q=0.8',
	'Connection': 'keep-alive'
}

s = requests.session()
s.headers.update(header)

pool = PooledDB(MySQLdb, 2, host='localhost', user='songkun', passwd='1qaz!QAZ', db='douban', port=3306, charset='utf8')
conn = pool.connection()
cur = conn.cursor()
# s.cookies.update(cookies)

# film = {}
user = {}

def get_all_user(url):
	_user = {}
	ct, next_page = get_collection_tab(url)
	_user.update(get_user(ct))
	while len(ct.find_all('table')) > 0:
		print next_page['href']
		ct, next_page = get_collection_tab(next_page['href'])
		_user.update(get_user(ct))
		time.sleep(1)
	return _user
	

def get_user(ct):
	_user = {}
	for tb in ct.find_all('table'):
		u_link = tb.tr.td.a['href']
		u = tb.tr.td.a.img['alt']
		_user.update({u_link: u})
	return _user

def get_collection_tab(url):
	r = s.get(url)
	soup = BeautifulSoup(r.content)
	collections_tab = soup.find('div', id='collections_tab')
	next_page = soup.find('div', class_='paginator').find('span', class_='next').a
	return collections_tab, next_page

def get_film_rating(grid_view):
	# _film = {}
	_film_rating = {}
	for item in grid_view.find_all('div', class_='item'):
		film_url = item.find('div', class_='info').ul.li.a['href']
		film_name = item.find('div', class_='info').ul.li.a.em.string
		rating = item.find('div', class_='info').ul.find_all('li')[2].span['class']
		# _film.update({film_url: film_name})
		_film_rating.update({film_url: rating})
	return _film_rating

def get_grid_view(url):
	r = s.get(url)
	soup = BeautifulSoup(r.content)
	grid_view = soup.find('div', class_='grid-view')
	next_page = soup.find('div', class_='paginator').find('span', class_='thispage').find_next()
	return grid_view, next_page

def get_all_film_rating(url):
    _fr = {}
    _url = url
    try:
        grid_view, next_page = get_grid_view(_url)
        _fr.update(get_film_rating(grid_view))
        while next_page.name == 'a':
			time.sleep(4)
			print next_page['href']
			_url = next_page['href']
			grid_view, next_page = get_grid_view(_url)
			_fr.update(get_film_rating(grid_view))
        return _fr, '', 'Y'
    except Exception, e:
		print e
		return _fr, _url, 'N'
    # finally:
		# return _fr, _url, 'N'

# write rating to file

# film_rating = get_all_film_rating(film_url, cookies)

'''
print '-------start get user info--------'
user = get_all_user(start_film_url)
user_list = []
for usr in user:
	user_list.append((usr.split('/')[4], usr, usr + 'collect/', 'N'))

cur.executemany('insert into user_url (user_id, user_movie_url, curr_url, is_finish) values (%s, %s, %s, %s)', user_list)
conn.commit()
cur.close()
conn.close()
print '-------- end get user info--------'
'''



'''
path = 'd:\\douban\\'
user_file_name = 'user.txt'
fp = open(path + user_file_name, 'w')
for dic in user:
	fp.write(dic)
	fp.write(':')
	fp.write(user[dic].encode('utf-8'))
	fp.write('\n')
fp.close()
'''

cur.execute('select user_id, curr_url from user_url t where t.is_finish = %s', 'N')
results = cur.fetchall()

for res in results:
	url = res[1]
	film_rating = {}
	rating = []
	film_rating, final_url, is_finish = get_all_film_rating(url)
	print final_url
	print is_finish
	for rt in film_rating:
		rating.append((res[0], rt.split('/')[4], film_rating[rt][0]))
	cur.executemany('insert into user_movie_rating (user_id, movie_id, rating) values (%s, %s, %s)', rating)
	conn.commit()
	cur.execute('update user_url t set t.curr_url = %s, t.is_finish = %s where t.user_id = %s', (final_url, is_finish, res[0]))
	conn.commit()
	
cur.close()
conn.close()
pool.close()

'''
for usr in user.keys():
	url = usr + 'collect/'
	film_rating = {}
	film_rating = get_all_film_rating(url, cookies)
	rating_file_name = 'rating_' + film_url.split('/')[4] + '.txt'
	fp = open(path + rating_file_name, 'w')
	for dic in film_rating:
		fp.write(dic)
		fp.write(':')
		fp.write(film_rating[dic][0])
		fp.write('\n')
	fp.close()
'''