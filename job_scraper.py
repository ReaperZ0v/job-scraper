from datetime import datetime 
from bs4 import BeautifulSoup
import pandas as pd 
import requests 
import glob
import os

def scrape_jobs(*args, **kwargs):
    page_range = kwargs['page_range']
    for x in range(1, int(page_range)):
        resp = requests.get(f"https://www.timesjobs.com/candidate/job-search.html?from=submit&actualTxtKeywords=python&searchBy=0&rdoOperator=OR&searchType=personalizedSearch&luceneResultSize=25&postWeek=60&txtKeywords=python&pDate=I&sequence={x}&startPage=1").text
        soup = BeautifulSoup(resp, "lxml")
        jobs = soup.find_all("li", class_="clearfix job-bx wht-shd-bx")

        posted = job.find("span", class_='sim-posted').text.strip()
        comp = job.find("h3", class_="joblist-comp-name")
        skills = job.find("span", class_='srp-skills').text.strip()

        data_exfiltrated = {
            "company": [company.text.strip() for company in comp],
            "skills": [skill for skill in skills]
            "posted": [post for post in posted],
        }

        now = datetime.now()
        print(f"[ALERT][{now}] Page {x} Data Exfiltrated to ===> scraped_jobs_page{x}.csv")

        jobs_dataframe = pd.DataFrame.from_dict(data_exfiltrated)
        jobs_dataframe.to_csv(f"scraped_jobs_page{x}.csv", index=False)

        company_data.clear()
        skills_data.clear()
        posted_data.clear()


if __name__ == "__main__":
    scrape_jobs(page_range=17)
    files = [i for i in glob.glob("*.csv")]

    merged_csv = pd.concat([pd.read_csv(f) for f in files])
    merged_csv.to_csv("scraped_jobs_data.csv", index=False)

    for file in files:
        if file == "scraped_jobs_data.csv":
            continue

        else:
            os.remove(file)

    df = pd.read_csv("scraped_jobs_data.csv")
    print(f"[SUCCESS] Total Data Exfiltrated is at a metric of {len(df)} rows")





    
