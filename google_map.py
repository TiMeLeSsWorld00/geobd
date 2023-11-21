import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

payload = {
	"source": "google",
	"url": f"https://www.google.com/search?tbs=lf:1,lf%5C_ui:9&tbm=lcl&q=restaurants+near+me#rlfi=hd:;si:;mv:[[54.6781006,25.2765623],[54.6672417,25.2563048]];tbs:lrf:!1m4!1u3!2m2!3m1!1e1!1m4!1u2!2m2!2m1!1e1!2m1!1e2!2m1!1e3!3sIAE,lf:1,lf_ui:9",
	"geo_location": "New York,New York,United States",
}


response = requests.request(
	"POST",
	"https://realtime.oxylabs.io/v1/queries",
	auth=("USERNAME", "PASSWORD"),
	json=payload,
	timeout=180,
)
html = response.json().get("results")[0].get("content")

name_selector = "[role='heading']"
type_selector = ".rllt__details div:nth-of-type(2)"
address_selector = ".rllt__details div:nth-of-type(3)"
hours_selectors = ".rllt__details div:nth-of-type(4)"
rating_count_selector = 'span:contains("(")'
rating_selector = "[aria-hidden='true']"
details_selector = ".rllt__details div:nth-of-type(5)"
price_selector = "span[aria-label*='xpensive']"
lat_selector = "[data-lat]"
lng_selector = "[data-lng]"


soup = BeautifulSoup(html, "html.parser")
data = []
for listing in soup.select("[data-id]"):
    place = listing.parent
    name_el = place.select_one(name_selector)
    name = name_el.text.strip() if name_el else ""

    rating_el = place.select_one(rating_selector)
    rating = rating_el.text.strip() if rating_el else ""

    rating_count_el = place.select_one(rating_count_selector)
    rating_count = ""
    if rating_count_el:
        count_match = re.search(r"\((.+)\)", rating_count_el.text)
    rating_count = count_match.group(1) if count_match else ""

    hours_el = place.select_one(hours_selectors)
    hours = hours_el.text.strip() if hours_el else ""

    details_el = place.select_one(details_selector)
    details = details_el.text.strip() if details_el else ""

    price_level_el = place.select_one(price_selector)
    price_level = price_level_el.text.strip() if price_level_el else ""

    lat_el = place.select_one(lat_selector)
    lat = lat_el.get("data-lat") if lat_el else ""

    lng_el = place.select_one(lng_selector)
    lng = lng_el.get("data-lng") if lng_el else ""

    type_el = place.select_one(type_selector)
    place_type = type_el.text.strip().split("·")[-1] if type_el else ""

    address_el = place.select_one(address_selector)
    address = address_el.text.strip() if address_el else ""

    place = {
        "name": name,
        "place_type": place_type,
        "address": address,
        "rating": rating,
        "price_level": price_level,
        "rating_count": rating_count,
        "latitude": lat,
        "longitude": lng,
        "hours": hours,
        "details": details,
    }
    data.append(place)


df = pd.DataFrame(data)
df.to_csv("data.csv", index=False)

