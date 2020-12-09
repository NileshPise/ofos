#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# -*- coding: utf-8 -*-
"""
Created on Fri Wed 5 02:20:22 2020
Project : Realistics Data Generation For Online food order system Project To Do Load Test
@author: Nilesh Pise

"""

from faker import Faker
from pymysql import connect
from pymysql import cursors
from pymysql.cursors import DictCursor
from datetime import datetime
from datetime import timedelta 
from datetime import date
from random import choice, randint
import pandas as pd
import numpy as np


fake = Faker('en_IN')

host = 'localhost'
user_db = 'root'
password_db = ''
db = 'food'
rows = input('Enter the no of records you want to insert.')

def phone_number(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)

def get_orders_id(customer_id, address, date):
    customer_id = customer_id
    address = address
    date = date

    try:
        connection = connect(host= host, user= user_db, password=password_db, db= db) 
        cursor = connection.cursor(DictCursor)
        sql_get_orders = "SELECT `id` FROM `orders` WHERE `customer_id`=%s AND `address`= %s AND `date`= %s"
        cursor.execute(sql_get_orders, (customer_id, address, date))
        result = cursor.fetchall()
        connection.commit()
        return choice(result)
    except:
        None


def get_user_id():
    
    try:
        connection = connect(host= host, user= user_db, password=password_db, db= db) 
        cursor = connection.cursor(DictCursor)
        sql_get_user = "SELECT `id` FROM `users`"
        cursor.execute(sql_get_user)
        result = cursor.fetchall()
        connection.commit()
        return choice(result)
        
    except:
        return {'id': 9}
    
def get_user_id1(contact, username, password):
    
    try:
        connection = connect(host= host, user= user_db, password=password_db, db= db) 
        cursor = connection.cursor(DictCursor)
        sql_get_user1 = "SELECT `id` FROM `users` WHERE contact= %s AND username= %s AND password= %s"
        cursor.execute(sql_get_user1, (contact, username, password))
        result = cursor.fetchall()
        connection.commit()
        return choice(result)
        
    except:
        return {'id': 9}

    
def get_wallet_id(customer_id):
    
    try:
        connection = connect(host= host, user= user_db, password=password_db, db= db) 
        cursor = connection.cursor(DictCursor)
        sql_get_wallet = "SELECT `id` FROM `wallet` WHERE `customer_id`= %s"
        cursor.execute(sql_get_wallet, (customer_id))
        result = cursor.fetchall()
        connection.commit()
        return choice(result)
        
    except:
        return {'id': 9}
    
    
def get_tickets_id(poster_id, date, subject):
    poster_id = poster_id 
    date = date
    subject = subject
    
    try:
        connection = connect(host= host, user= user_db, password=password_db, db= db) 
        cursor = connection.cursor(DictCursor)
        sql_get_tickets = "SELECT `id` FROM `tickets` WHERE poster_id= %s AND date= %s AND subject =%s"
        cursor.execute(sql_get_tickets,(poster_id, date, subject))
        result = cursor.fetchall()
        connection.commit()
        return choice(result)
        
    except:
        return {'id': 1}
    
def get_items_id():
    
    try:
        connection = connect(host= host, user= user_db, password=password_db, db= db) 
        cursor = connection.cursor(DictCursor)
        sql_get_items = "SELECT `id`, `price` FROM `items`"
        cursor.execute(sql_get_items)
        result = cursor.fetchall()
        connection.commit()
        return choice(result)
        
    except:
        return {'id': 1, 'price': 25}
    
    
def users():
    role = "Customer"
    name = fake.first_name() + " " + fake.last_name()
    username = name.split()[0] + str(phone_number(5))
    password = name.split()[1] + str(phone_number(7))
    email = name.split()[1] + str(phone_number(5)) + "@gmail.com"
    address = fake.address()
    contact = phone_number(10)
    verified = choice([0,1])

    try:
        
        connection = connect(host= host, user= user_db, password=password_db, db= db)
        cursor = connection.cursor()
        users_queery = "INSERT INTO `users`(`role`, `name`, `username`, `password`, `email`, `address`, `contact`, `verified`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(users_queery,(role, name, username, password, email, address, contact, verified))
        connection.commit()
    except:
        None
        
    try:
        wallet(contact= contact, username= username, password= password)
    except:
        None
        

    
def wallet(contact, username, password):
    custo = get_user_id1(contact, username, password)
    customer_id = custo['id']

    try:
        connection = connect(host= host, user= user_db, password=password_db, db= db)
        cursor = connection.cursor()
        wallet_queery = "INSERT INTO `wallet`(`customer_id`) VALUES (%s)"
        cursor.execute(wallet_queery,(customer_id))
        connection.commit()
    except:
        None
        
    try:
        wallet_details(customer_id= customer_id)
    except:
        None
        
        
        
def wallet_details(customer_id):
    wallet1 = get_wallet_id(customer_id= customer_id)
    wallet_id = wallet1['id']
    number = fake.credit_card_number()
    cvv = fake.credit_card_security_code()
    balance =  randint(500, 5000)
    
    try:
        connection = connect(host= host, user= user_db, password=password_db, db= db)
        cursor = connection.cursor()
        wallet_details_queery = "INSERT INTO `wallet_details`(`wallet_id`, `number`, `cvv`, `balance`) VALUES (%s,%s,%s,%s)"
        cursor.execute(wallet_details_queery,(wallet_id, number, cvv, balance))
        connection.commit()
    except:
        None
        
        
def order_details(item_id, quantity, price, order_id):
    order_id = order_id
    item_id = item_id
    quantity = quantity
    price = price
    
    try:

        connection = connect(host= host, user= user_db, password=password_db, db= db)
        cursor = connection.cursor()
        order_details_queery = "INSERT INTO `order_details`(`order_id`, `item_id`, `quantity`, `price`) VALUES (%s,%s,%s,%s)"
        cursor.execute(order_details_queery,(order_id, item_id, quantity, price))
        connection.commit()
    except:
        None
        
        
def orders():
    item1 = get_items_id()
    custo = get_user_id()
    customer_id = custo['id']
    address = fake.address()
    description = choice(['i want little bit spicy', 'please add red chili', 'dont add onion'])
    date = datetime.fromtimestamp(fake.date_time_between(start_date='-100d', end_date='now').timestamp())
    payment_type = choice(['Cash on Delivery', 'Wallet'])

    status = choice(['Yet to be delivered', 'Cancelled by Customer', 'Paused'])
    quantity = randint(1,4)
    price = item1['price'] * quantity
    item_id = item1['id']
    total = price
    
    try:
        connection = connect(host= host, user= user_db, password=password_db, db= db)
        cursor = connection.cursor()
        orders_queery = "INSERT INTO `orders`(`customer_id`, `address`, `description`, `date`, `payment_type`, `total`, `status`) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(orders_queery,(customer_id, address, description, date, payment_type, total, status))
        connection.commit()
    except:
        None

    try:
        
        order_id = get_orders_id(customer_id= customer_id, address=address, date= date)
        order_id = order_id['id']
        order_details(item_id = item_id, quantity= quantity, price= price, order_id= order_id)
    except:
        None


        
        
def ticket_details(ticket_id, user_id, description, date):
    ticket_id = ticket_id
    user_id = user_id
    description = description
    date = date

    try:

        connection = connect(host= host, user= user_db, password=password_db, db= db)
        cursor = connection.cursor()
        ticket_details_queery = "INSERT INTO `ticket_details`(`ticket_id`, `user_id`, `description`, `date`) VALUES (%s,%s,%s,%s)"
        cursor.execute(ticket_details_queery,(ticket_id, user_id, description, date))
        connection.commit()
    except:
        None
        
def tickets():
    poster_id1 = get_user_id()
    poster_id = poster_id1['id']
    subject = choice(['Payment', 'wallet', 'order'])
    description = subject + " problem is arising every time. while opening application"
    status = 'Answered'
    type1 = 'Support'
    date = datetime.fromtimestamp(fake.date_time_between(start_date='-100d', end_date='now').timestamp())
    
    try:

        connection = connect(host= host, user= user_db, password=password_db, db= db)
        cursor = connection.cursor()
        tickets_queery = "INSERT INTO `tickets`(`poster_id`, `subject`, `description`, `status`, `type`, `date`) VALUES (%s,%s,%s,%s,%s,%s)"
        cursor.execute(tickets_queery,(poster_id, subject, description, status, type1, date))
        connection.commit()
    except:
        None
    
    try:
        ticket_id2 = get_tickets_id(poster_id= poster_id, date= date, subject= subject)
        ticket_id = ticket_id2['id']
        ticket_details(ticket_id= ticket_id, user_id= poster_id, description= description, date= date)
    except:
        None
        
        
for i in range(0, int(rows)):
    users()
    tickets()
    orders()
    print("{} Records Inserted.".format(i))

