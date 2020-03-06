from flask import Flask, session
import pandas as pd 
from flask import Flask, make_response, request, url_for
import io
import csv
from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow
import simplejson
from flask import Flask,render_template,request,redirect
#from newsapi import NewsApiClient
from newsapi.newsapi_client import NewsApiClient
from geopy.geocoders import Bing
import math
import flask_login as flask_login
from flask_login import LoginManager, UserMixin
import requests
from timezonefinder import TimezoneFinder
from uszipcode import SearchEngine
import sys
sys.modules['pandas.io.json.normalize'] = pd.io.json._normalize
from pytrends.request import TrendReq
from alpha_vantage.timeseries import TimeSeries


login_manager = LoginManager()

application = Flask(__name__)

application.vars={}

application.secret_key = "_5#y2LhhhF4Q8zjnhjfjhr]/"

login_manager.init_app(application)

users = {'publicdataengine':{'pw':'C#2LHzp{'}}

class User(UserMixin):
  pass

@login_manager.user_loader
def user_loader(username):
  if username not in users:
    return

  user = User()
  user.id = username
  return user

@login_manager.request_loader
def request_loader(request):
  username = request.form.get('username')
  if username not in users:
    return

  user = User()
  user.id = username

  user.is_authenticated = request.form['pw'] == users[username]['pw']

  return user


#@app.route('/',methods=['GET','POST'])
#def start():
    #if request.method == 'GET':
        #return render_template('index2.html')

@application.route('/', methods=['GET', 'POST'])
def index():
  if request.method == 'POST':
    username = request.form.get('username')
    if request.form.get('pw') == users[username]['pw']:
      user = User()
      user.id = username
      flask_login.login_user(user)
      return redirect(url_for('start'))

  return render_template('index.html')

@application.route('/start')
@flask_login.login_required
def start():
  if request.method == 'GET':
        return render_template('index2.html')

@application.route('/logout')
def logout():
  flask_login.logout_user()
  return 'Logged out'

@application.route('/timezones',methods=['GET','POST'])
@flask_login.login_required
def timezones():
    
    if request.method == 'GET':
        return render_template('timezones.html')

    else:
        df = pd.read_csv(request.files.get("timezone_file"))
        df.index = df.timestamp
        df.index = pd.to_datetime(df.index)
        df.index = df.index.tz_localize('UTC')
        tf = TimezoneFinder(in_memory=True)
        timezones = []
        for index, row in df.iterrows():
            latitude = (row['latitude'])
            longitude = (row['longitude'])
            print(latitude,longitude)
            try:
                timezones.append(tf.timezone_at(lng=longitude, lat=latitude))
            except:
                timezones.append('N/A')
                
        df['timezone'] = timezones

        converted = []
        for index, row in df.iterrows():
            timezone = row['timezone']
            try:
                convert = index.tz_convert(timezone)
                converted.append(convert)
            except:
                converted.append('N/A')
                
        df['local_timestamp'] = converted
        df['local_time'] = df['local_timestamp'].astype(str).str[:19]
        #df = df.reset_index()
        print(df.columns)
        resp = make_response(df.to_csv())
        resp.headers["Content-Disposition"] = "attachment; filename=timezone_export.csv"
        resp.headers["Content-Type"] = "text/csv"
        return resp

@application.route('/weather',methods=['GET','POST'])
@flask_login.login_required

def weather():
    
    if request.method == 'GET':
        return render_template('genericweather.html')
    else:
        # request was a POST
        #app.vars['pandas'] = request.form['keyword']

        df3 = pd.read_csv(request.files.get("data_file"))
        print(len(df3))
    #api_key2 = request.form.get("api_key")
    #return "uploaded ok"
    #df['videoID'] = df['URL'].apply(getid)
    #vidids = df['videoID'].tolist()

        conversion_sum_col = []
        conversion_precipintensity_col = []
        conversion_precipprobability_col = []
        conversion_preciptype_col = []
        conversion_temp_col = []
        conversion_windspeed_col = []
        conversion_windgust_col = []
        conversion_cloudcover_col = []
        conversion_visibility_col = []
        day_summary_col = []
        day_precipprobability_col = []


        for index, row in df3.iterrows():
            date = row['time_string']
            lat = row['latitude']
            long = row['longitude']
            #address = row['address']
            #print(address)
            response = requests.get("https://api.darksky.net/forecast/6fd02b86b74c58ff6bf5b90ccb7b0fc6/%d,%d,%s?exclude=hourly,flags" % (lat, long, date))
            if response.status_code == 200:
                r = response.json()
                try:
                    conversion_sum = r['currently']['summary']
                except:
                    conversion_sum = "N/A"
                
                try:
                    conversion_precipintensity = r['currently']['precipIntensity']
                except: 
                    conversion_precipintensity ="N/A"
                try:
                    conversion_precipprobability = r['currently']['precipProbability']
                except:
                    conversion_precipprobability = "N/A"
                try:
                    conversion_preciptype = r['currently']['precipType']
                except:
                    conversion_preciptype = "N/A"
                else:
                    conversion_preciptype = 'N/A'
                try:
                    conversion_temp = r['currently']['temperature']
                except:
                    conversion_temp = "N/A"
                try:
                    conversion_windspeed = r['currently']['windSpeed']
                except: 
                    conversion_windspeed = "N/A"
                try:
                    conversion_windgust = r['currently']['windGust']
                except:
                    conversion_windgust = "N/A"
                try:
                    conversion_cloudcover = r['currently']['cloudCover']
                except:
                    conversion_cloudcover = "N/A"
                try:
                    conversion_visibility = r['currently']['visibility']
                except: 
                    conversion_visibility = "N/A"
                try:
                    day_summary = r['daily']['data'][0]['summary']
                except:
                    day_summary = "N/A"
                print(day_summary)
                try:
                    day_precipprobability = r['daily']['data'][0]['precipProbability']
                except:
                     day_precipprobability = "N/A"
                conversion_sum_col.append(conversion_sum)
                conversion_precipintensity_col.append(conversion_precipintensity)
                conversion_precipprobability_col.append(conversion_precipprobability)
                conversion_preciptype_col.append(conversion_preciptype )
                conversion_temp_col.append(conversion_temp)
                conversion_windspeed_col.append(conversion_windspeed)
                conversion_windgust_col.append(conversion_windgust)
                conversion_cloudcover_col.append(conversion_cloudcover)
                conversion_visibility_col.append(conversion_visibility)
                day_summary_col.append(day_summary)
                day_precipprobability_col.append(day_precipprobability)
            else:
                print("that didn't work :(")
                break

        df3['conversion_sum_col'] = conversion_sum_col
        df3['conversion_precipintensity_col'] = conversion_precipintensity_col
        df3['conversion_precipprobability_col'] = conversion_precipprobability_col
        df3['conversion_preciptype_col'] = conversion_preciptype_col
        df3['conversion_temp_col'] = conversion_temp_col
        df3['conversion_windspeed_col'] = conversion_windspeed_col
        df3['conversion_windgust_col'] = conversion_windgust_col
        df3['conversion_cloudcover_col'] = conversion_cloudcover_col
        df3['conversion_visibility_col'] =  conversion_visibility_col
        df3['day_summary_col'] =  day_summary_col
        df3['day_precipprobability_col'] = day_precipprobability_col

    #d = {(key, value) for (key, value) in izip(id1, cat)}
        

        df = df3.to_json()
        session['data'] = df
        resp = make_response(df3.to_csv())
        resp.headers["Content-Disposition"] = "attachment; filename=export.csv"
        resp.headers["Content-Type"] = "text/csv"
        return resp

@application.route('/social',methods=['GET','POST'])
@flask_login.login_required

def social():
    
    if request.method == 'GET':
        return render_template('social.html')

@application.route('/sports',methods=['GET','POST'])
@flask_login.login_required

def sports():
    
    if request.method == 'GET':
        return render_template('sports.html')

@application.route('/geocoding',methods=['GET','POST'])
@flask_login.login_required

def geocode():
    
    if request.method == 'GET':
        return render_template('geocoding.html')
    else:
        # request was a POST
        #app.vars['pandas'] = request.form['keyword']
        file = request.files.get("data_file")
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)

    #api_key2 = request.form.get("api_key")
    #return "uploaded ok"
    #df['videoID'] = df['URL'].apply(getid)
    #vidids = df['videoID'].tolist()

        g=Bing('AoGg6nfFsORF7WrNxkvEQpj2r-O7WTS5hoOYXg6fDynZIo4JkGcFZ-UPjPJ7HQda',timeout=5)

        ddict = {}
#create a list of adresses from your column name, then loop through that list
        adr = df['Address']
        for inputAddress in adr:
            #get the geocode & append it to list
            try:
                location = g.geocode(inputAddress, timeout=10)
                ddict[inputAddress] = [location.latitude, location.longitude]
            except: 
                ddict[inputAddress] = ["", ""]
                

        df2 = pd.DataFrame.from_dict(ddict, orient='index', columns=['lat', 'long']).reset_index()
        result = pd.merge(df, df2, how='left', left_on='Address', right_on='index').drop("index", 1)

        #df = df3.to_json()
        #session['data'] = df
        resp = make_response(result.to_csv())
        resp.headers["Content-Disposition"] = "attachment; filename=export.csv"
        resp.headers["Content-Type"] = "text/csv"
        return resp


@application.route('/zipcoding',methods=['GET','POST'])
@flask_login.login_required

def zipcode():
    
    if request.method == 'GET':
        return render_template('zipcoding.html')
    else:
        # request was a POST
        #app.vars['pandas'] = request.form['keyword']
        file = request.files.get("data_file")
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)

        g=Bing('AoGg6nfFsORF7WrNxkvEQpj2r-O7WTS5hoOYXg6fDynZIo4JkGcFZ-UPjPJ7HQda',timeout=5)

        df2 = df.drop_duplicates(subset=['latitude', 'longitude'], keep='first')

        reverse = df2[['latitude', 'longitude']]
        reverse_list = reverse.values.tolist()

        zips = []
        import geocoder # pip install geocoder
        for i in reverse_list:
            g = geocoder.bing(i, method='reverse', key='AoGg6nfFsORF7WrNxkvEQpj2r-O7WTS5hoOYXg6fDynZIo4JkGcFZ-UPjPJ7HQda')
            try:
                zips.append(g.postal)
            except:
                zips.append('N/A')
                
        reverse['zipcode'] = zips
        reverse = reverse.reset_index()
        #reverse = reverse.drop('action_timestamp', 1)

        result = pd.merge(df, reverse,  how='left', on=['latitude','longitude'])
        result
        resp = make_response(result.to_csv())
        resp.headers["Content-Disposition"] = "attachment; filename=export_zip.csv"
        resp.headers["Content-Type"] = "text/csv"
        return resp

@application.route('/demographic',methods=['GET','POST'])
@flask_login.login_required

def demographic():
    
    if request.method == 'GET':
        return render_template('demographic.html')
    else:
        # request was a POST
        #app.vars['pandas'] = request.form['keyword']
        file = request.files.get("data_file")
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)

        df2 = df.drop_duplicates(subset=['zipcode'], keep='first')

        df2 = df2[['zipcode']]

        search = SearchEngine(simple_zipcode=True) 

        results = []

        for i in df2.zipcode.tolist():
            zipcode = search.by_zipcode(i)
            dictionary = zipcode.to_dict()
            results.append(dictionary)
            
        zip_df = pd.DataFrame(results)
        zip_df['zipcode']=zip_df['zipcode'].astype(str)
        df['zipcode']=df['zipcode'].astype(str)
        result = pd.merge(df, zip_df,  how='left', on='zipcode')
        #final_df = final_df.drop_duplicates(subset =["local_timestamp","user_id"])
        resp = make_response(result.to_csv())
        resp.headers["Content-Disposition"] = "attachment; filename=export_zip.csv"
        resp.headers["Content-Type"] = "text/csv"
        return resp

@application.route('/news',methods=['GET','POST'])
@flask_login.login_required
def news():
    if request.method == 'GET':
        return render_template('news.html')
    else:
        # request was a POST
        application.vars['keyword'] = request.form['keyword']
        application.vars['earliest'] = request.form['earliest']
        application.vars['latest'] = request.form['latest']


        newsapi = NewsApiClient(api_key='dc919a5aeb324f01b0db89373fd71749')
        keyword = application.vars['keyword']
        oldest = application.vars['earliest']
        latest = application.vars['latest']
        print(keyword)


        articles_page = newsapi.get_everything(
                q=keyword,
                from_param=oldest,
                to=latest,
                language='en',
                sort_by='popularity')
        print(articles_page['totalResults'])
        total = articles_page['totalResults']
        maxpage = math.ceil(total/20)
        articles = []
        print(maxpage)

        for i in range(1,maxpage):
            articles_page = newsapi.get_everything(
                    q=keyword,
                    from_param=oldest,
                    to=latest,
                    language='en',
                    sort_by='popularity',
                    page=i)
            articles.extend(articles_page['articles'])
            
        articles_df = pd.DataFrame(articles)
        print(len(articles_df))
        print(articles_df)
        resp = make_response(articles_df.to_csv())
        resp.headers["Content-Disposition"] = "attachment; filename=export_news.csv"
        resp.headers["Content-Type"] = "text/csv"
        return resp


@application.route('/stocks',methods=['GET','POST'])
@flask_login.login_required
def stocks():
    if request.method == 'GET':
        return render_template('stocks.html')
    else:
        # request was a POST
        symbol = request.form['symbol']
        year = request.form['year']
        key = "YYMDSIFF67BSSI3G"

        def get_stocks (api_key, stock_symbol, year_filter):
            ts = TimeSeries(key=api_key, output_format='pandas')
            data, meta_data = ts.get_daily(symbol=stock_symbol, outputsize='full')
            data.index = pd.to_datetime(data.index)
            stock = data[(data.index.year == int(year_filter))]
            stock = stock.reset_index()
            stock = stock.sort_values(by=['date'])
            return stock

    
        data = get_stocks (key, symbol, year)
        resp = make_response(data.to_csv())
        resp.headers["Content-Disposition"] = "attachment; filename=export_stocks.csv"
        resp.headers["Content-Type"] = "text/csv"
        return resp


@application.route('/trends',methods=['GET','POST'])
@flask_login.login_required
def trends():
    if request.method == 'GET':
        return render_template('trends.html')
    else:
        # request was a POST
        keyword = request.form['keyword']
        market = request.form['market']
        market = market.upper()

        def get_trends (kw, language, market):
            pytrends = TrendReq(hl=language, tz=360)
            kw_list = [kw]
            pytrends.build_payload(kw_list, cat=0, timeframe='today 5-y', geo=market, gprop='')
            trends_df = pytrends.interest_over_time()
            trends_df = trends_df[[keyword]]
            return trends_df

    
        data = get_trends (keyword, 'en-US', market)
        resp = make_response(data.to_csv())
        resp.headers["Content-Disposition"] = "attachment; filename=export_trends.csv"
        resp.headers["Content-Type"] = "text/csv"
        return resp

#@app.route('/download', methods=["POST", "GET"])

def download_file():

    df3 = session.get('data', None)
    print(df3)
    file_ = pd.read_json(df3)
    resp = make_response(file_.to_csv())
    resp.headers["Content-Disposition"] = "attachment; filename=export.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp

    #print("yahyyyy")

def getid(url_str):
    return url_str.rsplit('/', 1)[-1]
    #stream = io.StringIO(f.stream.read().decode("UTF8"), newline=None)
    #csv_input = csv.reader(stream)
    #print("file contents: ", file_contents)
    #print(type(file_contents))
    #print(csv_input)
    #for row in csv_input:
        #print(row)

    #stream.seek(0)
    #result = transform(stream.read())

    #response = make_response(result)
    #response.headers["Content-Disposition"] = "attachment; filename=result.csv"
    #return response

if __name__ == "__main__":
    application.debug = True
    application.run(threaded=True)

