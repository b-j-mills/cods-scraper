import logging

from hdx.data.hdxobject import HDXError
from hdx.utilities.base_downloader import DownloadError


class COD:
    def __init__(self, downloader, dataset, country, ab_url, em_url, ps_url, errors):
        self.dataset = dataset
        self.iso = country["#country+code+v_iso3"]
        self.country = country["#country+name+preferred"]
        self.downloader = downloader
        self.errors = errors
        self.service_urls = {
            "ab": ab_url,
            "em": em_url,
            "ps": ps_url,
        }

    def get_service_resources(self, cod_type):
        resources = list()
        url = self.service_urls.get(cod_type)

        if cod_type in ["ab", "em"]:
            try:
                self.downloader.download(url + "?f=pjson")
            except DownloadError:
                self.errors.add(f"{self.iso}: Could not get data from {url}")

            service_list = self.downloader.get_json()["services"]
            for service in service_list:
                resource = dict()
                if service["name"].split("/")[1][:3].upper() != self.iso:
                    continue

                resource["url"] = url + "/" + service["name"].split("/")[1] + "/" + service["type"]
                resource["name"] = f"{service['name']} ({service['type']})"
                resource["format"] = "Geoservice"

                try:
                    self.downloader.download(resource["url"] + "?f=pjson")
                except DownloadError:
                    self.errors.add(f"{self.iso}: could not get data from {resource['download_url']}")
                    continue

                resource["description"] = self.downloader.get_json()["serviceDescription"]

                resources.append(resource)

        if cod_type == "ps":
            do_not_continue = False
            for adm in range(0, 5):
                resource = dict()
                if do_not_continue:
                    continue

                resource["url"] = url.replace("/iso", f"/{self.iso}").replace("/adm/", f"/{adm}/")
                resource["name"] = f"{self.iso.upper()} admin {adm} population"
                resource["format"] = "JSON"

                try:
                    self.downloader.download(resource["url"])
                except DownloadError:
                    do_not_continue = True
                    continue

                resource["description"] = f"{self.country} administrative level " \
                                          f"{adm} {self.downloader.get_json()['Year']} population statistics"

                resources.append(resource)

        return resources

    def remove_service_resources(self):
        updated = False
        for resource in reversed(self.dataset.get_resources()):
            if resource.get_file_type() not in ["geoservice", "json"]:
                continue
            if "itos.uga.edu" not in resource["url"]:
                continue

            try:
                self.dataset.delete_resource(resource, delete=False)
            except HDXError:
                self.errors.add(f"{self.dataset['name']}: Could not delete service resource")
                continue

            updated = True

        return updated

    def add_service_resources(self, resources):
        self.dataset.add_update_resources(resources)
        return
