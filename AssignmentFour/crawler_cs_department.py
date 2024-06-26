from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup
import re
from urllib.request import urlopen
from MongoDB import *


def scrape(db, url):
    global name, title, office, phone, email, web
    html = urlopen(url)
    soup = BeautifulSoup(html.read(), 'html.parser')
    professor_info = soup.find_all('div', class_='clearfix')
    seen_names = set()
    professor_doc_list = []
    unique_professors = []
    for professor in professor_info:
        name_element = professor.find('h2')
        if name_element:
            name = name_element.text.strip()
        else:
            name = ""

        title_element = professor.find('strong', string=re.compile("Title"))
        if title_element:
            title = title_element.next_sibling.strip()
        else:
            title = ""

        office_element = professor.find('strong', string=re.compile("Office"))
        if office_element:
            office = office_element.next_sibling.strip()
        else:
            office = ""

        phone_element = professor.find('strong', string=re.compile("Phone"))
        if phone_element:
            phone = phone_element.next_sibling.strip()
        else:
            phone = ""

        email_element = professor.find('a', href=lambda href: href and href.startswith('mailto:'))
        if email_element:
            email = email_element['href'].replace('mailto:', '')
        else:
            email = ""

        web_element = professor.find('a', href=lambda href: href and 'http' in href)
        if web_element:
            web = web_element['href']
        else:
            web = ""

        professor_doc = {
            "name": name,
            "title": title,
            "office": office,
            "phone": phone,
            "email": email,
            "website": web
        }
        professor_doc_list.append(professor_doc)
        if name == '':
            continue
        if name not in seen_names:
            seen_names.add(name)
            unique_professors.append(professor_doc)

    # Assuming db is your MongoDB collection
    for professor_doc in unique_professors:
        createDocument(db, professor_doc)
    return


def crawl(seed_url):
    # Creates frontier[], visited_urls[], and result_urls[]
    frontier = [seed_url]
    visited_urls = []
    result_urls = []
    # While loop that checks for values in frontier and if the list of visited_urls is not greater than 50.
    # This prevents the crawler from going through all the webpages and eventually getting lost.
    while frontier and len(visited_urls) < 50:
        # Removes the 1st URL out of frontier
        current_url = frontier.pop(0)
        try:
            # On the CPP website, many of the URLS do not have the header of https://www.cpp.edu so the program adds it
            # to the urls that are crawled and put into the frontier.

            # Opens the URL with BeautifulSoup
            html = urlopen(current_url)
            soup = BeautifulSoup(html.read(), 'html.parser')
            # Adds the values to the 'visited_urls' list
            visited_urls.append(current_url)

            # Search the HTML of the current URL for a title called "Permanent Faculty"
            # If title is not Permanent Faculty, the program will continue.
            if soup.find('title', string=re.compile("Permanent Faculty")):
                print("faculty page found:", current_url)
                # print(visited_urls, "\n", frontier)
                frontier.clear()
                return current_url
            # Searches through the HTML of the currentURL and grabs any URL that matches the tag of 'href'
            frontier_elements = soup.select("a[href]")
            url_start = 'https://www.cpp.edu/'
            # Iterates through the elements that matched the criteria of previous statement.
            for frontier_element in frontier_elements:
                # From the <a> tag, the crawler gets the attribute of href and sets it to the element 'new_url'
                new_url = frontier_element['href']
                new_url = url_start + new_url
                # Double checks to make sure that the URL has not been visited already and if the URL has not
                # been visited add it to the visited_urls list. If it was visited to skip it and move on to the
                # next URL.
                if new_url not in visited_urls and new_url not in frontier:
                    frontier.append(new_url)
                    result_urls.append(new_url)
        except AttributeError:
            pass
        except HTTPError:
            pass
        except URLError:
            pass
        except ValueError:
            pass
    return


def main():
    # Given a seed URL, the main function will call 'crawl' to start searching for the target: Permanent Faculty
    seed_url = "https://www.cpp.edu/sci/computer-science/ "
    url_want = crawl(seed_url)
    db = connectionDataBase()
    collection_name = "Professors"
    collection = db[collection_name]
    scrape(collection, url_want )


if __name__ == "__main__":
    main()
