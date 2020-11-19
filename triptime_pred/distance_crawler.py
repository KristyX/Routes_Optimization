from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import sys
from time import sleep


class DistanceCrawler():
    waiting_time = 3

    def __init__(self):
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--no-sandbox')
        self.options.add_argument("--headless")
        self.driver = webdriver.Chrome('./chromedriver_mac', chrome_options=self.options)
        self.actions = ActionChains(self.driver)
        self.wait = WebDriverWait(self.driver, self.waiting_time)

    def quit(self):
        self.driver.close()
        self.driver.quit()
        print('drivers have been killed')

    def get(self, start, end):
        url = 'https://www.google.com/maps/dir/' + start + '/' + end + '/data=!4m2!4m1!3e0'
        self.driver.get(url)
        xpath = "//div[@class='section-directions-trip clearfix selected']"
        condition = EC.visibility_of_element_located((By.XPATH, xpath))
        try:
            self.wait.until(condition)
            res = self.driver.find_element(By.XPATH, xpath).text.split("\n")[:2]
            print(res)
            return res
        except:
            print('DistanceCrawler: calling_url_error')
            '''
            self.driver.close()
            self.driver.quit()
            self.driver = webdriver.Chrome(chrome_options=self.options)
            res = ['none', 'none']
            '''
            pass
        finally:
            print(url)


if __name__ == "__main__":
    start = ['43.559626,-79.682648']
    end = ['43.567679,-79.660534']
    crawler = DistanceCrawler()
    for s, e in zip(start, end):
        crawler = DistanceCrawler()
        start_lat = s.split(",")[0]
        start_lng = s.split(",")[1]
        end_lat = e.split(",")[0]
        end_lng = e.split(",")[1]
        print(start_lat)
        print(end_lng)
        print(crawler.get(s, e))
    crawler.quit()
