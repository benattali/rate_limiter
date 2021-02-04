# -*- coding: utf-8 -*-

import os
from flask import Flask, jsonify, request
import sqlalchemy
from functools import wraps
import time
import pdb

# web app
app = Flask(__name__)

# database engine
engine = sqlalchemy.create_engine(os.getenv('SQL_URI'))

allowed_units = {'second': 1,
                 'minute': 60,
                 'hour': 3600,
                 'day': 86_400}

class Rule:
    num = 0
    unit = ''
    def __init__(self, num, unit):
        if unit not in allowed_units:
            raise ValueError("Invalid unit")
        self.num = num
        self.unit = unit

    def __str__(self):
        return f"{self.num} requests per {self.unit}"
    
    def __repr__(self):
        return self.__str__()

class MyRateLimiter:
    rules = {}
    ip_requests = {}

    def __init__(self, default_rules = []):
        self.default_rules = default_rules

    def limit(self, list_of_rules = []):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                endpoint = func.__name__
             
                if endpoint not in self.rules:
                    self.rules[endpoint] = list_of_rules or self.default_rules

                ip = request.remote_addr
                current_time = time.time()

                for endpoint, rule in self.rules.items():
                    rule_num = rule[0].num
                    rule_unit = rule[0].unit

                    if ip in self.ip_requests:
                        timestamps = self.ip_requests[ip]
                    else:
                        timestamps = []
                    timestamps.append(current_time)

                    conversion = allowed_units[rule_unit]
                    for request_time in timestamps:
                        if request_time < current_time - (rule_num * conversion):
                            timestamps.remove(request_time)
                    
                    if len(timestamps) <= rule_num:
                        self.ip_requests[ip] = timestamps
                    else:
                        raise Exception(f'Too many requests were sent (more than {rule[0]})')

                return func(*args, **kwargs)
            return wrapper
        return decorator
   
my_limit = MyRateLimiter(default_rules = [Rule(3, 'second'), Rule(5, 'minute')]) 


@app.route('/')
@my_limit.limit(list_of_rules = [Rule(20,'hour')])
def index():
    return 'Welcome to EQ Works 😎'


@app.route('/events/hourly')
@my_limit.limit()
def events_hourly():
    return queryHelper('''
        SELECT date, hour, events
        FROM public.hourly_events
        ORDER BY date, hour
        LIMIT 168;
    ''')



@app.route('/events/daily')
def events_daily():
    return queryHelper('''
        SELECT date, SUM(events) AS events
        FROM public.hourly_events
        GROUP BY date
        ORDER BY date
        LIMIT 7;
    ''')


@app.route('/stats/hourly')
def stats_hourly():
    return queryHelper('''
        SELECT date, hour, impressions, clicks, revenue
        FROM public.hourly_stats
        ORDER BY date, hour
        LIMIT 168;
    ''')


@app.route('/stats/daily')
def stats_daily():
    return queryHelper('''
        SELECT date,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(revenue) AS revenue
        FROM public.hourly_stats
        GROUP BY date
        ORDER BY date
        LIMIT 7;
    ''')

@app.route('/poi')
def poi():
    return queryHelper('''
        SELECT *
        FROM public.poi;
    ''')

def queryHelper(query):
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return jsonify([dict(row.items()) for row in result])
