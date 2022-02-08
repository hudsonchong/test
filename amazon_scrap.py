import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
import requests
import urllib
import datetime, random, time, pymysql

today_date = "\\" + str(datetime.date.today())
headers = {
    'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36'
}
doc_loc = [r"Y:\OM ONLY_Shared Documents\5 Reports with Power Query\Source Data\Amazon\Webscraping\Makeup" , 
           r"Y:\OM ONLY_Shared Documents\5 Reports with Power Query\Source Data\Amazon\Webscraping\Skincare",
           r"Y:\OM ONLY_Shared Documents\5 Reports with Power Query\Source Data\Amazon\Webscraping\Tools",
           r"Y:\OM ONLY_Shared Documents\5 Reports with Power Query\Source Data\Amazon\Webscraping\Footnail",
           r"Y:\OM ONLY_Shared Documents\5 Reports with Power Query\Source Data\Amazon\Webscraping\HairCare",
           r"Y:\OM ONLY_Shared Documents\5 Reports with Power Query\Source Data\Amazon\Webscraping\Bathnbathing"]


sheet_name = ['Makeup' , 'Skincare', 'ToolsNAccessories','FootHandNNailcare', 'HairCare',  'BathNBathingAcc']
#if link is broken, please update

def amazon_scrape(doc ,sheetname):
    df = pd.read_excel(r"Y:\OM ONLY_Shared Documents\5 Reports with Power Query\Source Data\Amazon\AmazonRankingURLs.xlsx", sheet_name=sheetname)
    df = df['URL']
    df_list = df.values.tolist()
    quote_page = df_list


    categories_lst, rankings_lst, names_lst, reviews_lst, num_of_reviews_lst, prices_lst, links_lst, images_lst, date_lst, asin_lst = [], [], [], [], [], [], [], [], [], []

    for url in quote_page:
        #page = requests.get(url)
        #print(page.content)
        page = urllib.request.urlopen(url)
        soup = BeautifulSoup(page.content, features='html.parser')
        print(soup)
        for contents in soup.find_all('li', attrs={'class': 'zg-item-immersion'}):
            try:
                category = soup.find('span', attrs={'class': 'category'})
                ranking = contents.find('span', attrs={'class': 'zg-badge-text'})
                name = contents.img['alt']
                review = contents.find('span', attrs={'class', 'a-icon-alt'})
                num_of_review = contents.find('a', attrs={'class', 'a-size-small a-link-normal'})
                price = contents.find('span', attrs={'class': 'p13n-sc-price'})
                link_raw = contents.find('a')
                link = link_raw.get('href')
                image = contents.img['src']
            except TypeError:                
                continue

            categories = None if category is None else category.text
            rankings = None if ranking is None else ranking.text.replace("#", "")
            names = None if name is None else name
            reviews = None if review is None else review.text[0:3]
            num_of_reviews = None if num_of_review is None else num_of_review.text.replace(",", "")
            prices = None if price is None else price.text.replace("$", "")
            links = None if link is None else "https://www.amazon.com/"+link
            images = None if image is None else image
            asins_raw = None if link is None else link.split("dp/")[1]
            asins = asins_raw.split("?")[0]

            categories_lst.append(str(categories)), rankings_lst.append(str(rankings)), names_lst.append(str(names)), reviews_lst.append(str(reviews)), asin_lst.append(str(asins)),
            num_of_reviews_lst.append(str(num_of_reviews)), prices_lst.append(str(prices)), links_lst.append(str(links)), images_lst.append(str(images)), date_lst.append(str(datetime.date.today()))
            time.sleep(random.uniform(.1, .35))

        time.sleep(random.uniform(1,1.8))

    df = pd.DataFrame({'ASIN':asin_lst, 'Category': categories_lst, 'Ranking': rankings_lst, 'Name': names_lst, 'Review': reviews_lst, 'Num_Of_Review': num_of_reviews_lst, 'Price': prices_lst, 'Link': links_lst, 'Image': images_lst, "Date": date_lst})

    export_csv = df.to_csv(doc+today_date+".csv", index = None, header = True)

    print("Scraped")

    return export_csv



#Insert CSV into MySQL today date file
def csv_to_mysql(doc , sheetname):
    mydb = pymysql.connect(host='10.1.7.109', user='ivyom', passwd='ivyom1212', db='om_raw')
    cursor = mydb.cursor()
    csv_data = pd.read_csv(open(
        doc + today_date + ".csv", encoding="utf8"),
                           header=0)

    for row in csv_data.iterrows():
        cursor.execute('''INSERT INTO amazon_ranking(ASIN, Category, Ranking, Name, Review, Num_Of_Review, Price, Link, Image, Date,User)
                      VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)''',
                       [row[1]['ASIN'], row[1]['Category'], row[1]['Ranking'], str(row[1]['Name']).encode('utf-8'), row[1]['Review'],
                        row[1]['Num_Of_Review'], row[1]['Price'], row[1]['Link'], row[1]['Image'], row[1]['Date'],'DD'])

    # close the connection to the database.
    mydb.commit()
    cursor.close()
    print("Imported")
i=0
for i in range(0,6,1):
    amazon_scrape(doc_loc[i], sheet_name[i])
    csv_to_mysql(doc_loc[i], sheet_name[i])

