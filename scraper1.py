import asyncio
import json
import re
import time
from datetime import datetime
from urllib.parse import urlparse
from urllib.parse import unquote

import aiohttp
import pandas as pd
from serpapi import GoogleSearch


class GMapExtractor:
    def __init__(self):
        self.api_key = "77a8f758e9799169762bdfb1c10b1217b63bb8eba0d59596ada753fe72f4a14b"

        # self.api_key = "YOUR GOOGLE API KEY"
        
    # Method to compare review count
    def compare_review(self, row):
        try:
            if int(row) > 20:
                return 1
            else:
                return 0
        except ValueError:
            return None

    # Method to extract city from address
    def city_address(self, val):
        if val is not None:
            parts = val.split(',')
            if len(parts) >= 2:
                return parts[-2].strip()
        return None
    
    # Method to extract state from address
    def state_address(self, val):
        if val is not None:
            parts = val.split(',')
            if len(parts) >= 2:
                state = parts[-1].strip()
                if len(state) == 2:  # Assuming state abbreviation
                    return state
        return None
    
    # Method to check if site matches any exception pattern
    def check_exemption(self, site):
        for pattern in exception_list:
            if '*' in pattern:
                pattern_regex = re.escape(pattern).replace(r'\*', '.*')
                if re.match(pattern_regex, site):
                    return 0
            elif pattern == site:
                return 0
        return 1

    # Method to format query name
    def fl_name(self, qr):
        parts = qr.split()
        if len(parts) > 1:
            return '_'.join(parts)
        else:
            return qr
        
    # Method to clean and format address
    def cleanse_address(self, val):
        if val is not None:
            return '="{}"'.format(val.split()[-1])

    # Method to sort out required data from the results
    def sort_out(self, sequence, key):
        collection = []
        for obj in sequence:
            form = obj
            if key in form.keys():
                if key == 'types':
                    collection.append(",".join(form[key]))
                elif key == "website":
                    collection.append(form[key])
                elif key == "address":
                    collection.append(form[key].title())
                else:
                    collection.append(form[key])
            else:
                collection.append(None)
        return collection

    # Async method to fetch data from URL
    async def fetch_url(self, url, session):
        async with session.get(url) as response:
            data = await response.json()
            return data

    # Async method to scrape Google map pack
    async def scrape_map_pack(self, queries, limit, local=False):
        async with aiohttp.ClientSession() as session:
            dataframes = []
            for qr in queries:
                params = {
                    "engine": "google_maps",
                    "q": f"{qr['query']}, {qr['city']}, {qr['state']}",
                    "type": "search",
                    "limit": limit,
                    "api_key": self.api_key,
                }

                params['q'] = f"{qr['query']} near, {qr['city']}, {qr['state']}"

                search = GoogleSearch(params)
                results = search.get_dict()
                local_results = results['local_results']

                df = pd.DataFrame({
                    "rank": self.sort_out(local_results, "position"),
                    "BusinessName": self.sort_out(local_results, "title"),
                    "BusinessAddress": self.sort_out(local_results, "address"),
                    "BusinessPhone": self.sort_out(local_results, "phone"),
                    "BusinessRating": self.sort_out(local_results, "rating"),
                    "BusinessWebsite": self.sort_out(local_results, "website"),
                    "keywords": self.sort_out(local_results, 'types'),
                    "NumberOfReviews": self.sort_out(local_results, "reviews"),
                    "GoogleSearchQuery": [f"{qr['query']},{qr['city']},{qr['state']}" for q in range(1, len(local_results) + 1)],
                    "ScrubbedDomain":[urlparse(u).netloc if u else "" for u in (item for item in self.sort_out(local_results, "website"))],
                })

                df['BusinessCity'] = df['BusinessAddress'].apply(self.city_address)
                df['BusinessState'] = df['BusinessAddress'].apply(self.state_address)
                df['BusinessZip'] = df['BusinessAddress'].apply(self.cleanse_address)
                df['Over20Reviews'] = df["NumberOfReviews"].apply(lambda x: 1 if x is not None and x >= 20 else 0)
                # Local column 1 if user city query is same as BusinessCity and ScrubbedDomain is not in exception list
                df['Local'] = df.apply(lambda row: 1 if pd.notna(row['BusinessAddress']) and qr['city'].replace(" ", "").lower() in row['BusinessAddress'].replace(" ", "").lower() and self.check_exemption(row['ScrubbedDomain']) and pd.notna(row['ScrubbedDomain']) else 0, axis=1)
                df["QueryDate/TimeStamp"] = [datetime.utcnow().strftime('%Y-%m-%d,%H:%M UTC') for x in range(1, len(local_results) + 1)]
                dataframes.append(df)

            df = pd.concat(dataframes, ignore_index=True)
            fl = self.fl_name(qr['query'])
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            df.head(limit).to_csv(f"{fl}_{qr['city']}_{qr['state']}_Map_{datetime.now().strftime('%d%m%Y')}.csv", encoding='utf-8-sig', index=False)
        

    # Method to get search ranking
    async def get_search_ranking(self, query, limit):
        if query.__len__() > 1:
            data_frames = []
            for qr in query:
                params = {
                    "engine": "google",
                    "q": f"{qr['query']},{qr['city']},{qr['state']}",
                    "api_key": self.api_key,
                    "num": limit
                }

                search = GoogleSearch(params)
                results = search.get_dict()
                organic_results = results["organic_results"]

                df = pd.DataFrame({
                    "rank": [res['position'] for res in organic_results],
                    "BusinessDomain":[urlparse(u).netloc if u else "" for u in (item["link"] for item in organic_results)],
                    "BusinessName": [res['source'] for res in organic_results],
                    "PageTitle": [res['title'] for res in organic_results],
                    "BusinessURL": [str(unquote(res['displayed_link'])) for res in organic_results],
                    "GoogleSearchQuery": [params["q"] for x in range(1, len(organic_results) + 1)],
                    "QueryDate/TimeStamp": [datetime.utcnow().strftime('%Y-%m-%d,%H:%M UTC') for x in range(1, len(organic_results) + 1)]
                })

                df['BusinessCity'] = [qr['city'] for x in range(1, len(organic_results) + 1)]
                df['BusinessNiche'] = [qr['query'] for x in range(1, len(organic_results) + 1)]
                df['BusinessState'] = [qr['state'] for x in range(1, len(organic_results) + 1)]
                data_frames.append(df)

            df = pd.concat(data_frames, ignore_index=True)
            fl = self.fl_name(qr['query'])
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            df.to_csv(f"{fl}_{qr['city']}_{qr['state']}_Organic_{datetime.now().strftime('%d%m%Y')}.csv", encoding='utf-8-sig', index=False)
        else:
            params = {
                "engine": "google",
                "q": f"{query[0]['query']}, {query[0]['city']}, {query[0]['state']}",
                "api_key": self.api_key,
                "num": limit
            }
            
            search = GoogleSearch(params)
            results = search.get_dict()
            organic_results = results["organic_results"]

            df = pd.DataFrame({
                "rank": [res['position'] for res in organic_results],
                "BusinessDomain":[urlparse(u).netloc if u else "" for u in (item["link"] for item in organic_results)],
                "BusinessName": [res['source'] for res in organic_results],
                "PageTitle": [res['title'] for res in organic_results],
                "BusinessURL": [str(unquote(res['displayed_link'])) for res in organic_results],
                "GoogleSearchQuery": [params["q"] for x in range(1, len(organic_results) + 1)],
                "QueryDate/TimeStamp": [datetime.utcnow().strftime('%Y-%m-%d,%H:%M UTC') for x in range(1, len(organic_results) + 1)]
            })
            df['BusinessCity'] = [query[0]['city'] for x in range(1, len(organic_results) + 1)]
            df['BusinessNiche'] = [query[0]['query'] for x in range(1, len(organic_results) + 1)]
            df['BusinessState'] = [query[0]['state'] for x in range(1, len(organic_results) + 1)]
            fl = self.fl_name(query[0]['query'])
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            df.to_csv(f"{fl}_{query[0]['city']}_{query[0]['state']}_Organic_{datetime.now().strftime('%d%m%Y')}.csv", encoding='utf-8-sig', index=False)


async def main(cities, states, qrs, limit, local=False):
    obj = GMapExtractor()
    queries = [{"city": c, "state": s, "query": q} for c, s, q in zip(cities.split(","), states.split(","), qrs.split(","))]

    await obj.scrape_map_pack(queries, limit, local)
    print("[+] Map Packs exported...✅")

    await obj.get_search_ranking(queries, limit)
    print("[+] Search Ranking exported...✅")


if __name__ == "__main__":
    try:
        with open('ExceptionList.txt', 'r') as file:
            lines = file.readlines()

        exception_list = [line.strip() for line in lines if line.strip()]

        while True:
            try:
                limit = int(input("Enter # of search results to get from Google > "))
                break
            except ValueError:
                print("Invalid input for the number of search results. Please enter a valid integer.")

        city = input("Enter cities (separate by comma) > ")
        state = input("Enter states (separate by comma) > ")
        query = input("Enter Queries (separate by comma) > ")

        asyncio.run(main(city, state, query, limit))

        time.sleep(3)

    except KeyboardInterrupt:
        print("\nExit Program")

    except FileNotFoundError:
        print("Exception list 'ExceptionList.txt' NOT FOUND! \t\nMake sure to put Exception list and exe in same folder!\n")
