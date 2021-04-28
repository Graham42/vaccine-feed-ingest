#!/usr/bin/env python

import asyncio
import json
import pathlib
import re
import sys
from typing import List

from aiofile import async_open
from bs4 import BeautifulSoup
from fetch import location_file_name_for_url


async def parse_location(input_file: pathlib.Path) -> dict:
    result: dict = {"phone-numbers": [], "contact-links": []}
    async with async_open(input_file, "r") as f:
        contents = await f.read()
        doc = BeautifulSoup(contents, "html.parser")
    if doc is None:
        raise Exception("failed to set up beautiful soup")

    citation_el = doc.find("meta", attrs={"property": "ga:citation:metadata"})
    if citation_el is None:
        raise Exception(
            "'ga:citation:metadata' meta tag not found, this element is used for a stable id"
        )

    result["node-id"] = citation_el.attrs["internal_url"]
    result["last-updated"] = citation_el.attrs["last_updated"]

    main_el = doc.find(id="main-content")
    if main_el is None:
        raise Exception("main content block has changed classes")

    phone_numbers_el = main_el.find(class_="contact-phone-numbers")
    if phone_numbers_el is not None:
        for contact_phone_el in phone_numbers_el.find_all(class_="contact-phone"):
            phone_number = {}
            a = contact_phone_el.find("a")
            if a is not None and a.attrs["href"] is not None:
                phone_number["href"] = a.attrs["href"]
            label = contact_phone_el.find(class_="contact-phone__label")
            if label is not None:
                phone_number["label"] = label.text.strip()
            result["phone-numbers"].append(phone_number)

    for link in main_el.find_all(class_="contact__link"):
        for a in link.find_all("a"):
            result["contact-links"].append(
                {"href": a.attrs["href"], "label": a.text.strip()}
            )

    return result


async def parse_landing(input_dir: pathlib.Path) -> List:
    locations_path = input_dir / "locations.html"

    async with async_open(locations_path, "r") as f:
        contents = await f.read()
        doc = BeautifulSoup(contents, "html.parser")
    if doc is None:
        raise Exception("failed to set up beautiful soup")

    header_cols = doc.select("#datatable > thead > tr > th")
    headers = [h.text.strip() for h in header_cols]
    if not re.search(r"Location Name", headers[0], re.IGNORECASE):
        raise Exception(
            "datatable has changed column header 'Location Name', column order may have changed"
        )
    if not re.search(r"County", headers[1], re.IGNORECASE):
        raise Exception(
            "datatable has changed column header 'County', column order may have changed"
        )
    if not re.search(r"Address", headers[2], re.IGNORECASE):
        raise Exception(
            "datatable has changed column header 'Address', column order may have changed"
        )

    location_rows = doc.select("#datatable > tbody > tr")
    async def parse_row(row):
        cells = row.find_all("td")
        location = {
            # these first 3 items are for backwards compatibility with the
            # previous parser iteration
            "Location Name": cells[0].text.strip(),
            "County": cells[1].text.strip(),
            "Address": cells[2].text.strip(),
            "address-parts": {},
        }
        # inside the address column the data is organized with classes
        # containing semantic parts: address-line1, locality, postal-code, etc
        for span in cells[2].find_all("span"):
            # these spans should only ever have 1 class, but just in case,
            # convert the list to a string
            key = " ".join(span.attrs["class"])
            location["address-parts"][key] = span.text.strip()

        a = row.find("a")
        if a is not None and a.attrs["href"] is not None:
            file_name = location_file_name_for_url(a.attrs["href"])
            extras = await parse_location(input_dir / file_name)
            location.update(extras)
        return location
    futures = [parse_row(row) for row in location_rows]
    locations = await asyncio.gather(*futures)

    # locations = []
    # for row in location_rows:
    #     cells = row.find_all("td")
    #     location = {
    #         # these first 3 items are for backwards compatibility with the
    #         # previous parser iteration
    #         "Location Name": cells[0].text.strip(),
    #         "County": cells[1].text.strip(),
    #         "Address": cells[2].text.strip(),
    #         "address-parts": {},
    #     }
    #     # inside the address column the data is organized with classes
    #     # containing semantic parts: address-line1, locality, postal-code, etc
    #     for span in cells[2].find_all("span"):
    #         # these spans should only ever have 1 class, but just in case,
    #         # convert the list to a string
    #         key = " ".join(span.attrs["class"])
    #         location["address-parts"][key] = span.text.strip()

    #     a = row.find("a")
    #     if a is not None and a.attrs["href"] is not None:
    #         file_name = location_file_name_for_url(a.attrs["href"])
    #         extras = await parse_location(input_dir / file_name)
    #         location.update(extras)
    #     locations.append(location)
    return locations


async def main():
    output_dir = pathlib.Path(sys.argv[1])
    input_dir = pathlib.Path(sys.argv[2])

    locations = await parse_landing(input_dir)

    out_filepath = output_dir / "locations.parsed.ndjson"

    async with async_open(out_filepath, "w") as f:
        for obj in locations:
            await f.write(json.dumps(obj))
            await f.write("\n")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
