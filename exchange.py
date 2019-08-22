
import requests
import pandas as pd
from fake_useragent import UserAgent
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

def daum_exchanges():
    url = "https://finance.daum.net/api/exchanges/summaries"
    headers = {
        'User-Agent': UserAgent().chrome,
        'referer': "https://finance.daum.net/exchanges",
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    result = data["data"]
    columns = ["country", "currencyName", "basePrice", "change", "changePrice", "cashBuyingPrice", "cashSellingPrice"]
    df = pd.DataFrame(result)[columns]
    df["change"] = df["change"].apply(lambda data: "-" if data == "FALL" else "+")
    df["changePrice"] = df["change"] + df["changePrice"].astype("str")
    df.drop(columns="change", inplace=True)
    return df

base = declarative_base()

class ExchangeDaum(base):
    
    __tablename__ = "daum"
    
    id = Column(Integer, primary_key=True)
    country = Column(String(50), nullable=False)
    currencyName = Column(String(50), nullable=False)
    basePrice = Column(Float, nullable=False)
    changePrice = Column(Float, nullable=False)
    cashBuyingPrice = Column(Float, nullable=False)
    cashSellingPrice = Column(Float, nullable=False)
    rdate = Column(TIMESTAMP, nullable=False)

    def __init__(self, country, currencyName, basePrice, changePrice, cashBuyingPrice, cashSellingPrice):
        self.country = country
        self.currencyName = currencyName
        self.basePrice = basePrice
        self.changePrice = changePrice
        self.cashBuyingPrice = cashBuyingPrice
        self.cashSellingPrice = cashSellingPrice
        
    def __repr__(self):
        return "<ExchangeDaum country:{}, currencyName:{}, basePrice:{},\
changePrice:{}, cashBuyingPrice:{}, cashSellingPrice:{}".format(
            self.country, self.currencyName, self.basePrice, 
            self.changePrice, self.cashBuyingPrice, self.cashSellingPrice,
        )
    
class SaveDatabase:

    def __init__(self, base, df, ip="13.209.190.69", pw="yky2158021", database="exchange"):
        self.mysql_client = create_engine("mysql://root:{}@{}/{}?charset=utf8".format(pw, ip, database))
        self.datas = df.to_dict("records")
        self.base = base
        
    def mysql_save(self):
        
        # make table
        self.base.metadata.create_all(self.mysql_client)
        
        # parsing keywords
        results = [ExchangeDaum(**data) for data in df.to_dict("records")]

        # make session
        maker = sessionmaker(bind=self.mysql_client)
        session = maker()

        # save datas
        session.add_all(results)
        session.commit()

        # close session
        session.close()
        
        print("saved!")
        
# crawling datas
df = daum_exchanges()

# make save databse object
sd = SaveDatabase(base, df)

# save database
sd.mysql_save()
