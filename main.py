import aiohttp
from bs4 import BeautifulSoup
import asyncio
from time import sleep

headers = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}


async def get_fortune_1000_companies(session):
    company_list = []
    url = 'https://cyber.harvard.edu/archived_content/people/edelman/fortune-registrars/fortune-list.html'
    async with session.get(url, headers=headers, ssl=False) as response:
        text = await response.read()
        if response.status == 200:
            soup = BeautifulSoup(text.decode('utf-8'), 'html5lib')
            for table_row in soup.find_all('tr'):
                cells = table_row.findAll('td')
                company_name = cells[1].text.strip()
                if company_name:
                    company_list.append(company_name)
            return company_list[1:]


def get_overview_page_tasks(session, list):
    tasks = []
    for item in list:
        search_result_url = f"https://www.glassdoor.com/Search/results.htm?keyword={item}"
        tasks.append(asyncio.create_task(session.get(search_result_url, headers=headers,
                                                     ssl=False, timeout=1000, )))
    return tasks


async def parse_overview_page_tasks(tasks):
    results = []
    for resp in tasks:
        if resp.status == 200:
            text = await resp.text()
            soup = BeautifulSoup(text,  'html.parser')
            link = soup.select_one(".company-tile")
            company_name = soup.select_one("h3.d-sm-block")
            print(company_name)
            if link:
                results.append({"url": "http://www.glassdoor.com" +
                                link.get('href'), "name": company_name.text if company_name else ""})
    return results


def get_benefits_page_tasks(session, list):
    tasks = []
    for item in list:
        tasks.append(asyncio.create_task(session.get(item['url'], headers=headers,
                                                     ssl=False, timeout=1000, )))
    return tasks


async def parse_benefits_page_tasks(tasks):
    benefits_page_urls = []
    for resp in tasks:
        if resp.status == 200:
            text = await resp.text()
            soup = BeautifulSoup(text,  'html.parser')
            benefits_link = soup.select_one('a.eiCell.cell.benefits')
            company_name = soup.select_one("#DivisionsDropdownComponent")
        if benefits_link and benefits_link.attrs:
            benefits_page_urls.append(
                {"name": company_name.text if company_name else "", "url": "http://www.glassdoor.com"+benefits_link.attrs['href']})
    return benefits_page_urls


def get_pto_tasks(session, list):
    tasks = []
    for item in list:
        tasks.append(asyncio.create_task(session.get(item['url'], headers=headers,
                                                     ssl=False, timeout=600, )))
    return tasks


async def parse_for_unlimited_pto(tasks):
    unlimited_pto = []
    for resp in tasks:
        if resp.status == 200:
            text = await resp.text()
            soup = BeautifulSoup(text,  'html.parser')
            company_name = soup.select_one("#DivisionsDropdownComponent")

            if "unlimited time off" in text.lower() or "unlimited paid time off" in text.lower() or "unlimited pto" in text.lower() or "unlimited vacation" in text.lower():
                unlimited_pto.append(
                    company_name.text.strip() if company_name else "",)
    return unlimited_pto


async def main():
    async with aiohttp.ClientSession() as session:
        list = await get_fortune_1000_companies(session)

        try:
            overview_page_tasks = get_overview_page_tasks(session, list)
            overview_page_task_results = await asyncio.gather(*overview_page_tasks)
            overview_page_urls = await parse_overview_page_tasks(overview_page_task_results)

            benefits_page_tasks = get_benefits_page_tasks(
                session, overview_page_urls)
            benefits_page_task_results = await asyncio.gather(*benefits_page_tasks)
            benefits_page_urls = await parse_benefits_page_tasks(benefits_page_task_results)

            pto_tasks = get_pto_tasks(session, benefits_page_urls)
            pto_tasks_results = await asyncio.gather(*pto_tasks)
            pto = await parse_for_unlimited_pto(pto_tasks_results)

            print(pto)

        except Exception as ex:
            print(str(ex))

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()