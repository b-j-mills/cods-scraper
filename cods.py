import logging

from hdx.data.hdxobject import HDXError
from hdx.utilities.base_downloader import DownloadError


class COD:
    def __init__(self, downloader, ab_url, em_url, ps_url, errors):
        self.downloader = downloader
        self.service_urls = {
            "ab": ab_url,
            "em": em_url,
            "ps": ps_url,
        }
        self.errors = errors

    def get_boundary_jsons(self):
        boundary_jsons = dict()
        for cod_type in ["ab", "em"]:
            url = self.service_urls.get(cod_type)
            try:
                service_json = self.downloader.download_json(url + "?f=pjson")
            except DownloadError:
                self.errors.add(f"Could not get data from {url}")
                return boundary_jsons

            boundary_jsons[cod_type] = service_json

        return boundary_jsons

    def get_service_resources(self, boundary_jsons, country, cod_type):
        iso = country["#country+code+v_iso3"]
        country_name = country["#country+name+preferred"]
        resources = list()
        url = self.service_urls.get(cod_type)

        if cod_type in ["ab", "em"]:
            service_list = boundary_jsons[cod_type].get("services")
            if not service_list:
                self.errors.add(f"{iso}: could not find service list")
                return resources

            for service in service_list:
                resource = dict()
                if service["name"].split("/")[1][:3].upper() != iso:
                    continue

                resource["url"] = url + "/" + service["name"].split("/")[1] + "/" + service["type"]
                resource["name"] = f"{service['name']} ({service['type']})"
                resource["format"] = "Geoservice"

                try:
                    resource_desc = self.downloader.download_json(resource["url"] + "?f=pjson")
                except DownloadError:
                    self.errors.add(f"{iso}: could not get data from {resource['download_url']}")
                    continue

                resource["description"] = resource_desc.get("serviceDescription")
                resources.append(resource)

        if cod_type == "ps":
            do_not_continue = False
            for adm in range(0, 5):
                resource = dict()
                if do_not_continue:
                    continue

                resource["url"] = url.replace("/iso", f"/{iso}").replace("/adm/", f"/{adm}/")
                resource["name"] = f"{iso.upper()} admin {adm} population"
                resource["format"] = "JSON"

                try:
                    year = self.downloader.download_json(resource["url"])
                except DownloadError:
                    do_not_continue = True
                    continue

                year = year.get("Year")
                if not year:
                    continue

                resource["description"] = f"{country_name} administrative level {adm} {year} population statistics"
                resources.append(resource)

        return resources

    def remove_service_resources(self, dataset):
        updated = False
        for resource in reversed(dataset.get_resources()):
            if resource.get_file_type() not in ["geoservice", "json"]:
                continue
            if "itos.uga.edu" not in resource["url"]:
                continue

            try:
                dataset.delete_resource(resource, delete=False)
            except HDXError:
                self.errors.add(f"{dataset['name']}: Could not delete service resource")
                continue

            updated = True

        return dataset, updated

    def add_service_resources(self, dataset, resources):
        try:
            dataset.add_update_resources(resources)
        except HDXError:
            self.errors.add(f"{dataset['name']}: Could not add service resource")
            return None

        return dataset
