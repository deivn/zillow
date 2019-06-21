#! /usr/bin/env python  
# -*- coding:utf-8 -*-  
import json

class Data(object):

    def __init__(self, price, bedroom, bathroom, street, deal_type, img_url,
                 living_sqft, comments, agent, house_type, heating, cooling,
                 price_sqft, year_build, parking, lot_sqft, hoa_fee,
                 contact_phone, contact_name, contact_email, url, mls,
                 apn, garage, deposit, zip, latitude, longitude, city, state):
        self.price = price
        self.bedroom = bedroom
        self.bathroom = bathroom
        self.street = street
        self.deal_type = deal_type
        self.img_url = img_url
        self.living_sqft = living_sqft
        self.comments = comments
        self.agent = agent
        self.house_type = house_type
        self.heating = heating
        self.cooling = cooling
        self.price_sqft = price_sqft
        self.year_build = year_build
        self.parking = parking
        self.lot_sqft = lot_sqft
        self.hoa_fee = hoa_fee
        self.contact_phone = contact_phone
        self.contact_name = contact_name
        self.contact_email = contact_email
        self.url = url
        self.mls = mls
        self.apn = apn
        self.garage = garage
        self.deposit = deposit
        self.zip = zip
        self.latitude = latitude
        self.longitude = longitude
        self.city = city
        self.state = state

    def data2dict(self):
        data = {
            "price": self.price,
            "bedroom": self.bedroom,
            "bathroom": self.bathroom,
            "street": self.street,
            "deal_type": self.deal_type,
            "img_url": self.img_url,
            "living_sqft": self.living_sqft,
            "comments": self.comments,
            "agent": self.agent,
            "house_type": self.house_type,
            "heating": self.heating,
            "cooling": self.cooling,
            "price_sqft": self.price_sqft,
            "year_build": self.year_build,
            "parking": self.parking,
            "lot_sqft": self.lot_sqft,
            "hoa_fee": self.hoa_fee,
            "contact_phone": self.contact_phone,
            "contact_name": self.contact_name,
            "contact_email": self.contact_email,
            "url": self.url,
            "mls": self.mls,
            "apn": self.apn,
            "garage": self.garage,
            "deposit": self.deposit,
            "zip": self.zip,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "city": self.city,
            "state": self.state
        }
        return data

    def dict2str(self):
        return json.dumps(self.data2dict())
    


