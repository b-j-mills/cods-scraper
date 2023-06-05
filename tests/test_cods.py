from json import load
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.useragent import UserAgent

from cods import COD


class TestCOD:
    country = {
        "#country+code+v_iso3": "POL",
        "#country+name+preferred": "Poland",
    }
    with open(join("tests", "fixtures", "COD_External_Edgematch.json")) as openfile:
        data = load(openfile)
    boundary_jsons = {
        "em": data,
    }
    service_resources_em = [
        {'url': 'https://gistmaps.itos.uga.edu/arcgis/rest/services/COD_External_Edgematch/POL_PL_Edgematch/MapServer',
         'name': 'COD_External_Edgematch/POL_PL_Edgematch (MapServer)',
         'format': 'Geoservice',
         'description': 'This map service contains an edgematched version of OCHA Common Operational Datasets for Poland: Administrative Boundaries. The service is available as ESRI Map, ESRI Feature, WMS, and KML Services. See the OCHA COD/FOD terms of use for access and use constraints.'},
    ]
    service_resources_ps = [
        {'url': 'https://apps.itos.uga.edu/CODV2API/api/v1/themes/cod-ps/lookup/Get/0/do/POL',
         'name': 'POL admin 0 population',
         'format': 'JSON',
         'description': 'Poland administrative level 0 2022 population statistics'},
        {'url': 'https://apps.itos.uga.edu/CODV2API/api/v1/themes/cod-ps/lookup/Get/1/do/POL',
         'name': 'POL admin 1 population',
         'format': 'JSON',
         'description': 'Poland administrative level 1 2022 population statistics'},
    ]

    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def downloader(self):
        class Download:
            @staticmethod
            def download_json(url):
                if "COD_External" in url:
                    filename = "POL_PL_Edgematch.json"
                else:
                    adm = url.split("/")[-3]
                    if int(adm) > 1:
                        return dict()
                    filename = f"cod-ps-pol_adm{adm}.json"

                with open(join("tests", "fixtures", filename)) as openfile:
                    data = load(openfile)
                return data

        return Download()

    @pytest.fixture(scope="class")
    def cod(self, configuration):
        cod = COD(
            self.downloader,
            configuration["ab_url"],
            configuration["em_url"],
            configuration["ps_url"],
            ErrorsOnExit(),
        )
        return cod

    def test_get_service_resources_em(self, downloader, configuration):
        cod = COD(downloader, configuration["ab_url"], configuration["em_url"], configuration["ps_url"], ErrorsOnExit())
        service_resources = cod.get_service_resources(self.boundary_jsons, self.country, "em")
        assert service_resources == self.service_resources_em

    def test_get_service_resources_ps(self, downloader, configuration):
        cod = COD(downloader, configuration["ab_url"], configuration["em_url"], configuration["ps_url"], ErrorsOnExit())
        service_resources = cod.get_service_resources(self.boundary_jsons, self.country, "ps")
        assert service_resources == self.service_resources_ps

    def test_remove_service_resources_em(self, downloader, configuration):
        cod = COD(downloader, configuration["ab_url"], configuration["em_url"], configuration["ps_url"], ErrorsOnExit())
        indataset = Dataset.load_from_json(join("tests", "fixtures", "cod-em-pol.json"))
        outdataset = Dataset.load_from_json(join("tests", "fixtures", "cod-em-pol_remove_resources.json"))
        dataset, updated = cod.remove_service_resources(indataset)
        assert updated is True
        assert dataset.get_resources() == outdataset.get_resources()

    def test_remove_service_resources_ps(self, downloader, configuration):
        cod = COD(downloader, configuration["ab_url"], configuration["em_url"], configuration["ps_url"], ErrorsOnExit())
        indataset = Dataset.load_from_json(join("tests", "fixtures", "cod-ps-pol.json"))
        outdataset = Dataset.load_from_json(join("tests", "fixtures", "cod-ps-pol_remove_resources.json"))
        dataset, updated = cod.remove_service_resources(indataset)
        assert updated is False
        assert dataset.get_resources() == outdataset.get_resources()

    def test_add_service_resources_em(self, downloader, configuration):
        cod = COD(downloader, configuration["ab_url"], configuration["em_url"], configuration["ps_url"], ErrorsOnExit())
        indataset = Dataset.load_from_json(join("tests", "fixtures", "cod-em-pol_remove_resources.json"))
        outdataset = Dataset.load_from_json(join("tests", "fixtures", "cod-em-pol_add_resources.json"))
        service_resources = cod.get_service_resources(self.boundary_jsons, self.country, "em")
        dataset = cod.add_service_resources(indataset, service_resources)
        assert dataset.get_resources() == outdataset.get_resources()

    def test_add_service_resources_ps(self, downloader, configuration):
        cod = COD(downloader, configuration["ab_url"], configuration["em_url"], configuration["ps_url"], ErrorsOnExit())
        indataset = Dataset.load_from_json(join("tests", "fixtures", "cod-ps-pol_remove_resources.json"))
        outdataset = Dataset.load_from_json(join("tests", "fixtures", "cod-ps-pol_add_resources.json"))
        service_resources = cod.get_service_resources(self.boundary_jsons, self.country, "ps")
        dataset = cod.add_service_resources(indataset, service_resources)
        assert dataset.get_resources() == outdataset.get_resources()
