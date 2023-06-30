import logging
from os.path import join, expanduser

from cods import COD

from hdx.api.configuration import Configuration
from hdx.location.country import Country
from hdx.data.hdxobject import HDXError
from hdx.data.dataset import Dataset
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-cods"


def main(**ignore):
    configuration = Configuration.read()

    with ErrorsOnExit() as errors:
        with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
            cod = COD(
                downloader,
                configuration["ab_url"],
                configuration["em_url"],
                configuration["ps_url"],
                errors,
            )
            boundary_jsons = cod.get_boundary_jsons()
            if len(boundary_jsons) < 2:
                cod.errors.add("Could not get boundary service data")
                return

            countries = Country.countriesdata()["countries"]
            dataset_types = ["ab", "em", "ps"]

            for country in countries:
                for dataset_type in dataset_types:
                    dataset = Dataset.read_from_hdx(f"cod-{dataset_type}-{country.lower()}")
                    if not dataset:
                        continue

                    logger.info(f"Starting to update {country} {dataset_type.upper()} dataset")
                    service_resources = cod.get_service_resources(boundary_jsons, countries[country], dataset_type)
                    dataset, updated = cod.remove_service_resources(dataset)

                    if len(service_resources) == 0 and not updated:
                        continue

                    if len(service_resources) > 0:
                        dataset = cod.add_service_resources(dataset, service_resources)

                    if not dataset:
                        continue

                    try:
                        dataset.update_in_hdx(
                            hxl_update=False,
                            batch_mode="KEEP_OLD",
                            updated_by_script="HDX Scraper: CODS",
                            remove_additional_resources=True,
                            ignore_fields=["num_of_rows", "resource:description"],
                        )
                    except HDXError as ex:
                        errors.add(f"{dataset['name']}: {ex}")


if __name__ == "__main__":
    facade(
        main,
        hdx_site="prod",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
