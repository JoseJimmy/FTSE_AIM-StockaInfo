import scrapy


from scrapy.loader import ItemLoader
from bs4 import BeautifulSoup

class LseSpider(scrapy.Spider):


    name = 'hlinfoAIM'
    allowed_domains = ['hl.co.uk']
    # FirstPage= 'https://www.londonstockexchange.com/indices/ftse-350/constituents/table'
    # start_urls = [FirstPage] +['https://www.londonstockexchange.com/indices/ftse-350/constituents/table?page=%d'% (page) for page in range(2,19)]

    start_urls = ['https://www.hl.co.uk/shares/stock-market-summary/ftse-aim-100']



    def parse(self, response):
        baseurl='https://www.hl.co.uk'

        soup = BeautifulSoup(response.body, 'html.parser')

        print('>>>I Level Got repnse %s from %s'%(response.status,response.url))
        #
        # filename = response.url.split("/")[-2]+'stage1.html'
        # with open(filename, 'wb') as f:
        #     f.write(response.body)


        table = soup.find(class_="stockTable")
        stcklinks=[]
        for idx,row in enumerate(table.find_all('tr')):
            if(len(row.find_all('td'))==6):
                link=baseurl+row.find_all('a')[0].get('href')
                stcklinks.append(link)
        for link in  stcklinks:
            yield scrapy.Request(link, callback=self.parse_item)



    def parse_item(self, response):
        print('>>>>>>>>>>II Level Got repnse %s from %s'%(response.status,response.url))

        soup = BeautifulSoup(response.body, 'html.parser')

        # filename = response.url.split("/")[-2]+'stage2.html'
        # with open(filename, 'wb') as f:
        #     f.write(response.body)

        links = soup.find_all('a')
        for link in links:
            if (link.get('href') is not None):
                temp = link.get('href')
                if ('company-information' in temp):
                    complink = temp
                    print(complink, '-', len(complink))
                    yield scrapy.Request(complink,  callback = self.parse_details)


    def parse_details(self, response):

        soup = BeautifulSoup(response.body, 'html.parser')
        print('>>>>>>>III Level Got repnse %s from %s'%(response.status,response.url))
        # filename = response.url.split("/")[-2]+'stage3.html'
        # with open(filename, 'wb') as f:
        #     f.write(response.body)


        for y in soup.find_all('p'):
            if(y.parent.parent.find('h2') is not None):
                txt = y.parent.parent.find('h2').text.strip()
                if('Business summary'  in txt):
                    Bussinfo=y.text.strip()
                    print(Bussinfo)
        met_tab=soup.find_all('dl',class_="spacer-top")
        props=[]
        for row in met_tab[0].find_all('dt'):
                props.append(row.text.split(':')[0].strip())
        val = []
        for row in met_tab[0].find_all('dd'):
            val.append(row.text.strip())
        metaval=''

        for idx,v in enumerate(val):
            metaval=metaval+props[idx]+':'+v+'\n'
        epic=val[0]
        metaval = metaval.encode('utf-8').decode('utf-8')
        Bussinfo = Bussinfo.encode('utf-8').decode('utf-8')

        # yield(metaval,Bussinfo)
        yield {
            'EPIC' :epic,
            'metaval': metaval,
            'Bussinfo': Bussinfo
            }