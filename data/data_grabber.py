import obspy
import requests
import io
import matplotlib
import pandas as pd
import helpers
def get_event_response(starttime, endtime, network, station, channel):
    """get an event mseed file from the knmi database given
    query parameters describing the event"""
    baseurl = 'http://rdsa.knmi.nl/fdsnws/dataselect/1/query'
    payload = {'starttime':starttime,'endtime':endtime,'network':network,
               'station':station, 'channel':channel,'nodata':404}
    r = requests.get(baseurl,params=payload)
    print(r.status_code)
    return {'data':r.content,'starttime':starttime,'endtime':endtime
            ,'channel':channel,'station':station}
def streamify(event_response):
    data = event_response['data']
    st = obspy.read(io.BytesIO(data))
    useful_keys = ['starttime','endtime','channel','station']
    streamified = {'stream':st}
    for k in useful_keys:
        streamified[k]=event_response[k]
    return streamified
get_and_streamify = helpers.pipe(get_event_response, streamify) #pipe(f,g)() is equivalent to f(g())
if __name__ == '__main__':
	tstart='2018-02-18T14:05:10.2'
	tend = '2018-02-18T14:05:15.2'
	network = 'NL'
	station= 'VKB'
	channel = 'BHZ'
	streamified = get_and_streamify(tstart, tend, network, station, channel)
	print(streamified)