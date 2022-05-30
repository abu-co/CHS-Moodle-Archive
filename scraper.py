#!/usr/bin/env python3
# File name: scraper.py
# Description: Scraper script for CHS Moodle before it gets taken down
# Author: https://github.com/abu-co/
# Year of creation: 2022

import argparse
from datetime import datetime
from enum import Enum
import os
import re
from urllib.parse import urlparse, parse_qs, quote
from sys import stderr
from typing import Literal, Tuple, Union, cast
import requests
import bs4
from bs4 import BeautifulSoup, ResultSet, Tag


class ScrapeConfig:
    asession_key = ('ASPSESSIONIDQCQCBTTR', 'ASPSESSIONIDSCTBCSSQ')[1] # apparently this can change...
    msession_key = 'MoodleSession'

    headers: dict[str, str] = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Cache-Control": "max-age=0",
        "Referer": "http://web3.carlingfor-h.schools.nsw.edu.au/"
    }


    def __init__(self) -> None:
        self.asession: Union[str, None] = None
        # self.dir = "Courses/"
        self.outdir: Union[str, None] = None
        self.msession = str()
        self.preview = True
        self.url = str()
        self.timeout = -1
        self.maxtry = -1
        self.debug = False


    def add_cookies(self, session: requests.Session) -> None:
        if self.asession:
            session.cookies.set(self.asession_key, self.asession)
        session.cookies.set(self.msession_key, self.msession)

    @staticmethod
    def parse_arguments():
        config = ScrapeConfig()

        example = 'Example: py scraper.py -ms "a1b2c3d4e5f5g7h8l9k0" '
        example += '"http://web3.carlingfor-h.schools.nsw.edu.au/' + \
            'applications/moodle2/course/view.php?id=412"'

        args = argparse.ArgumentParser(description="Scrapes Moodle!", epilog=example)

        args.add_argument(
            "-as", "--asession", help="ASP session cookies value", type=str, required=False)
        args.add_argument(
            "-ms", "--msession", help="Moodle session cookies value", type=str, required=True)

        args.add_argument(
            "-out", "--outdir", help="Custom output directory", type=str, required=False)

        args.add_argument(
            "-t", "--timeout", help="The connection timeout", type=int, default=5
        )
        args.add_argument(
            "--maxtry", help="Maximum retry count", default=-1
        )

        args.add_argument(
            "--preview", help="Previews scraping without download", action="store_true")
        args.add_argument(
            "--debug", help="Debug mode", action="store_true"
        )
        
        args.add_argument("url", help="The URL to scrape", type=str)
        # args.add_argument(
        #     "dir", help="The Moodle 'directory' of this page, e.g. Courses/Mathematics", type=str)

        parsed_args = args.parse_args()

        if parsed_args.outdir:
            parsed_args.outdir = str(parsed_args.outdir).replace(os.sep, '/')

        for attr in vars(config):
            value = getattr(parsed_args, attr)
            if attr not in ["asession", "outdir"]:
                assert value is not None
            setattr(config, attr, value)
        
        # if not config.dir.startswith("Courses/"):
        #     print("Error: page dir should always start with \"Courses/\"!")
        #     exit(-1)

        return config


def is_carlo_url(url: str) -> bool:
    return "web3.carlingfor-h.schools.nsw.edu.au" in url

def spam_get_request(
    config: ScrapeConfig,
    session: requests.Session,
    url: str,
    headers: Union[None, dict[str, str]] = None,
    verbose = True,
    head_only = False
):
    return spam_request(
        "head" if head_only else "get", config, session, url, headers, verbose = verbose
    )

def spam_request(
    method: Literal["get", "head", "post"],
    config: ScrapeConfig,
    session: requests.Session,
    url: str,
    headers: Union[None, dict[str, str]] = None,
    data: Union[None, dict[str, str]] = None,
    verbose = True,
    return_new_location = False
) -> Tuple[Union[requests.Response, None], int]:
    def perform_request():
        methods = {
            "get": session.get,
            "head": session.head,
            "post": session.post
        }
        return cast(requests.Response, methods[method](
            url, headers=headers, timeout=config.timeout, allow_redirects=False, data=data
        ))

    if not is_carlo_url(url):
        print(">>> ERROR: trying to fetch non-carlo url:", url)
        return (None, 0)

    url = url.replace("https://", "http://")

    if verbose:
        print("\t[fetch] %s" % (method.upper()), url)

    count = 0
    while count < config.maxtry or config.maxtry < 0:
        count += 1
        try:
            result = (perform_request(), count)
            if result[0].is_redirect and (new_url := result[0].headers.get("Location")):
                if return_new_location:
                    return (cast(requests.Response, new_url), count) # ugly hack...
                return spam_request(method, config, session, new_url, headers, data, verbose, return_new_location)
            return result
        except requests.Timeout:
            if verbose:
                print(">>> Timed out: Retrying (%d)..." % count)
    print(">>> FAIL: Reached Max Trial Count (%d)!" % config.maxtry)
    return (None, count)


class TopicResourceType(Enum):
    UNSUPPORTED = 0,
    NORMAL = 1,
    FOLDER = 2,
    LINK = 3


class TopicResource:
    __icon_exts = {
        "icon": ".svg",
        "f/pdf-24": ".png",
        "f/document-24": ".png",
    }

    __supported_res_urls = {
        "moodle2/mod/resource/": TopicResourceType.NORMAL,
        "moodle2/mod/folder/": TopicResourceType.FOLDER,
        "moodle2/mod/url/": TopicResourceType.LINK
    }

    def __init__(self, name: str, url: str) -> None:
        self.name = name
        self.url = url
        self.icon_url: Union[str, None] = None
        self.indentation: int = 0
        # self.type: Union[str, None] = None
        self.filename: Union[str, None] = None

        self.is_empty_folder = False
        self.link_content: str = str()
        
        for frag in self.__supported_res_urls.keys():
            if frag in self.url:
                self.type = self.__supported_res_urls[frag]
                break
        else:
            self.type = TopicResourceType.UNSUPPORTED

    
    def get_id(self) -> str:
        id = parse_qs(urlparse(self.url).query).get("id")
        assert id
        return id[0]

    
    @property
    def is_proper_resourse(self) -> bool:
        return self.type != TopicResourceType.UNSUPPORTED
    

    def get_local_filename(self) -> str:
        # id = self.get_id()
        # assert self.type
        # type = mimetypes.guess_extension(self.type)
        # assert type
        # return id + type
        if not self.filename:
            if self.type == TopicResourceType.LINK:
                return self.link_content
            print(f"FATAL ERROR: Invalid filename for {self.name}: {self.filename}")
            assert self.filename
        return self.filename
    

    def get_local_icon_path(self, output_dir: str) -> Union[str, None]:
        base_path = os.path.relpath("./media", "./" + output_dir).replace(os.sep, '/') + '/'
        if self.type == TopicResourceType.FOLDER:
            return base_path + "folder.svg"
        if self.type == TopicResourceType.LINK:
            return base_path + "link.svg"
        if self.icon_url:
            query = parse_qs(urlparse(self.icon_url).query)
            # print(query)
            # return re.sub(
            #     r"[^a-zA-Z0-9_\-\.]",  
            #     '_',
            #     query)
            image_name = query["image"][0]
            if not image_name:
                return None
            return base_path + \
                image_name + self.__icon_exts[image_name]

        return None
    
    def download(self, config: ScrapeConfig, session: requests.Session, output_dir: str, head_only = False) -> int:
        print(f">> Fetching \"{self.url}\"...")

        response: requests.Response

        if not config.debug:
            print_fail = lambda r: \
                print(">>> ERROR: Failed to fetch resourse for %s: %d!" % 
                    (self.name, (cast(requests.Response, r).status_code if r else -1)), 
                    file=stderr)

            resp = spam_request(
                "head" if (head_only and self.type == TopicResourceType.NORMAL) else "get", 
                config, session, self.url, 
                return_new_location = self.type == TopicResourceType.LINK
                )[0]
            if not resp:
                print_fail(resp)
                return 0

            if self.type == TopicResourceType.NORMAL:
                response = resp
            elif self.type in [ TopicResourceType.FOLDER, TopicResourceType.LINK ]:
                if isinstance(resp, str):
                    if self.type != TopicResourceType.LINK:
                        print(">>> How did this happen for %s?! Non-school link!" % self.name)
                    self.type = TopicResourceType.LINK
                    self.link_content = resp
                    print(">>> Got link:", self.link_content)
                    return 0
                
                soup = BeautifulSoup(resp.content, "html.parser")

                if self.type == TopicResourceType.FOLDER:
                    if head_only:
                        self.filename = self.name + "-" + datetime.today().strftime('%Y%m%d') + '.zip'
                        print(">>> Unable to check folders using HEAD:" + 
                            " assuming to be %s and skipping..." % self.filename)
                        if not os.path.exists(output_dir + '/' + self.filename):
                            # print(">>> WARNING: couldn't find local copy! Assuming to be empty...")
                            # self.is_empty_folder = True
                            pass
                        else:
                            return 0

                    form_action = "https://web3.carlingfor-h.schools.nsw.edu.au/applications/moodle2/mod/folder/download_folder.php"
                    form = soup.find("form", {"action": form_action})
                    if not form:
                        print(">>> Reached empty folder \"%s\"!" % self.name)
                        # print(resp.content)
                        self.is_empty_folder = True
                        return 0

                    post_data: dict[str, str] = {}
                    i: Tag
                    for i in cast(Tag, form).find_all("input", {"type": lambda v: v != "submit"}):
                        post_data[i.attrs["name"]] = i.attrs["value"]
                    
                    fresp = spam_request(
                        "post", config, session, form_action, None, post_data)[0]
                    if not fresp:
                        print_fail(fresp)
                        return 0
                    response = fresp
                else: # link
                    wrapper = soup.find("div", class_="urlworkaround")
                    if not wrapper or (a := wrapper.find("a")) is None:
                        print(">>> ERROR: failed to parse link for %s!" % self.name)
                        self.link_content = "#failed_to_scrape"
                        return 0
                    self.link_content = cast(Tag, a).attrs["href"]
                    print(">>> Scraped link resource:", self.link_content)
                    return 0
            else:
                print(">>> WARNING: Unsupported resource:", self.name)
                return 0
        else:
            response = requests.Response()
            response.headers["Content-Disposition"] = r'inline; filename="2020 TRIAL SOLUTIONS.pdf"'
            response._content = b"Testing... Testing..."
            response.headers["Content-Length"] = str(len(response._content))
            response.status_code = 200
        
        # Content-Disposition: inline; filename="2020 TRIAL SOLUTIONS.pdf"
        disposition = response.headers["Content-Disposition"]
        if not response.ok or not disposition:
            print(">>> ERROR: Invalid response received!", file=stderr)
            return 0;

        self.filename = cast(re.Match[str], re.search(
            r'filename\s*?=\s*?"([^"]+)"', disposition)).group(1)
        output_file = output_dir + '/' + self.get_local_filename()
        file_size = response.headers.get("Content-Length")

        if not self.filename:
            print("Error parsing filename from", disposition)
            return 0
        
        exists = os.path.exists(output_file)
        if not head_only:
            if exists:
                print(">>> WARNING: File already exists!", file=stderr)
                return int(file_size) if file_size else 0

            raw_buffer = response.content
            file_size = int(file_size) if file_size else len(raw_buffer)

            with open(output_file, "wb") as output:
                print(">>> Outputting to \"%s\"..." % output_file)
                output.write(raw_buffer)

            return file_size
        elif not exists:
            return self.download(config, session, output_dir, False) # download missing...
        return 0


class Topic:
    def __init__(self, name: str) -> None:
        self.name = name
        self.resources: list[TopicResource] = []
    
    @property
    def is_builtin(self) -> bool:
        return self.name == "General"


class Page:
    output_markdown_filename = "README.md"

    def __init__(self, title: str) -> None:
        self.title = title
        self.topics: list[Topic] = []
        self.__output_dir = "moodle"


    @staticmethod
    def scrape_page(config: ScrapeConfig, session: requests.Session):
        if not config.debug:
            config.add_cookies(session)

            response, trial_count = spam_get_request(
                config,
                session,
                config.url,
                headers=config.headers
            )
            
            if response is None:
                print(f"Too many ({trial_count}) failed trials!", file=stderr)
                return None;

            if not response.ok:
                print("Failed to fetch: ", response.status_code, file=stderr)
                return None

            # print(response.text[:128])

            soup = BeautifulSoup(response.content, "html.parser")
        else:
            with open("test.html", "r", encoding="utf-8") as file:
                soup = BeautifulSoup(file.read(), "html.parser")

        page_title = soup.select_one(".page-header-headings > h1")
        if not page_title:
            print("Page without title reached!")
            exit(-2)

        print("Scraping", page_title.text + "...")

        output_dir: str

        active_node = soup.select_one("p.active_tree_node")
        assert active_node is not None

        escape = lambda p: re.sub(r"[^a-zA-Z0-9_ \-%@!~`\(\)\[\]\:\,\.\?\{\}=+\$]", '-', p)

        if config.outdir:
            output_dir = config.outdir
        else:
            output_dir = escape(cast(Tag, active_node.find("a")).text)
            while True:
                parent_list = active_node.find_parent("ul", role="group")
                if parent_list is None:
                    print("ERROR: Failed to detect couse output directory.")
                    print("\tPlease specify it manually using -out/--outdir.")
                    exit(-2)
                parent_node = cast(Tag, parent_list.find_previous_sibling(
                    "p", class_=["tree_item", "branch"]))
                dir_name = parent_node.text
                output_dir = escape(dir_name) + '/' + output_dir
                if dir_name == "Courses":
                    break
                active_node = parent_node
            output_dir = 'moodle/' + output_dir
        
        print(f'Outputting to "{output_dir}"...')

        topic_list: bs4.ResultSet[bs4.Tag] = soup.find("ul", class_="topics") \
            .find_all("li", class_=["section", "main"]) # type: ignore

        topics: list[Topic] = []

        for topic in topic_list:
            section_name: bs4.Tag = topic.find("h3", class_="sectionname")  # type: ignore

            activity_instances: ResultSet[Tag] = topic.findAll("div", class_="activityinstance")

            topic_data = Topic(section_name.text)

            indent_extraction_regex = r"mod-indent-(\d+)";

            for activity_instance in activity_instances:
                indent_div = cast(Tag, activity_instance.parent).find_previous_sibling(
                    "div", class_=lambda c: bool(c and ("mod-indent-" in c)))
                indentation = re.search(
                    indent_extraction_regex,
                    ' '.join(cast(Tag, indent_div).attrs["class"])
                ) if indent_div else None

                link: Tag = activity_instance.find("a")  # type: ignore
                name: Tag = link.find("span", class_="instancename")   # type: ignore

                res = TopicResource(
                    " ".join(cast(bs4.ResultSet[bs4.NavigableString], name.find_all(text=True, recursive=False))), 
                    link.attrs["href"].replace("https://", "http://")
                )

                icon: Tag = link.find("img")  # type: ignore
                if icon is not None:
                    res.icon_url = icon.attrs["src"]

                res.indentation = int(indentation.group(1)) if indentation else 0
                # print(">>> indent:", res.indentation)

                topic_data.resources.append(res)
            
            topics.append(topic_data)
            print(f"> {topic_data.name} ({len(topic_data.resources)})")

        page = Page(page_title.text)

        page.topics = topics
        page.__output_dir = output_dir

        return page


    def create_dir(self, config: ScrapeConfig) -> bool:
        exists = os.path.exists(self.__output_dir)
        if not config.preview and not exists:
            os.makedirs(self.__output_dir, exist_ok=True)
        return not exists


    def output_markdown(self, config: ScrapeConfig):
        if config.preview:
            return
        output_filename = self.__output_dir + '/' + self.output_markdown_filename
        if os.path.exists(output_filename) and not os.access(output_filename, os.W_OK):
            print("Markdown generation aborted: No write access!")
            return
        with open(output_filename, 'w') as output:
            output.write("# " + self.title + '\n\n')
            for topic in self.topics:
                if not topic.is_builtin:
                    output.write("## " + topic.name + '\n\n')
                for res in topic.resources:
                    if res.indentation:
                        output.write("&emsp;" * res.indentation)
                    icon_url = res.get_local_icon_path(self.__output_dir)
                    link_md =  f"{f'![]({icon_url}) ' if icon_url else ''}{res.name}"
                    if res.is_empty_folder:
                        link_md += " `(Empty)`"
                    elif res.is_proper_resourse and not topic.is_builtin:
                        local_href = quote(res.get_local_filename(
                        )) if res.type != TopicResourceType.LINK else res.link_content
                        link_md = "[" + link_md + f"]({local_href})"
                    output.write(link_md + '\n\n')
            # output.writelines(lines)
        print("Markdown is output to " + output_filename + '.')


    def download(self, config: ScrapeConfig, session: requests.Session, head_only = False) -> int:
        if config.preview:
            print("Preview only: no download occurs.")
            return 0
        
        if head_only:
            print("Directory already exists: enabling HEAD only mode...")
        
        download_size = 0

        resource_count = 0
        for topic in self.topics:
            for res in topic.resources:
                if not res.is_proper_resourse:
                    continue
                download_size += res.download(config, session, self.__output_dir, head_only = head_only)
                resource_count += 1
                # break ####
        print("> Fetched %d resources." % resource_count)

        return download_size


def main() -> None:
    """
    R.I.P. Moodle.  
    But before you depart...
    """

    config = ScrapeConfig.parse_arguments()

    session = requests.Session();

    page = Page.scrape_page(config, session)
    if page is None:
        print("ERROR: Failed to fetch page.", file=stderr)
        return

    bytes_downloaded = page.download(config, session, head_only = not page.create_dir(config))
    print(f"Downloaded {bytes_downloaded} bytes ({bytes_downloaded // (1024 ** 2)} MB).")

    page.output_markdown(config)


if __name__ == '__main__':
    main()
